# Data model and reporting assumptions

## Published datasets

All names below are prefixed with `workspace.`. Materialized-view definitions are preserved from the running Databricks pipeline.

| Schema | Dataset | Logical grain | Source / purpose |
| --- | --- | --- | --- |
| `kkbox_silver` | `members` | Original member record; duplicates retained | `kkbox_raw.members_v3`; typed member attributes, original values, and quality flags |
| `kkbox_silver` | `subscription_transactions` | Original transaction record; duplicates retained | Union of `transactions` and `transactions_v2`; payments, renewal and cancellation state |
| `kkbox_silver` | `daily_listening_activity` | Member/activity date is the intended grain; source rows retained | Union of `user_logs` and `user_logs_v2`; playback buckets and valid listening duration |
| `kkbox_gold` | `source_coverage` | Source category | Date bounds and retained row counts for members, listening, and transactions |
| `kkbox_gold` | `signups` | Month | Observed registration counts, including zero-filled months |
| `kkbox_gold` | `registration_channels` | Month/channel code | Observed registration method counts |
| `kkbox_gold` | `engagement` | Month | MAU, average DAU, stickiness, playtime, and duration coverage |
| `kkbox_gold` | `listening_quality` | Month | Play-weighted near-complete/early-stop signals and bucket coverage |
| `kkbox_gold` | `completion_mix` | Month/completion bucket | Five playback bucket shares, with counts for weighted calculations |
| `kkbox_gold` | `subscriptions` | Month | Modelled subscription churn/retention and monetization estimates |
| `kkbox_gold` | `cohorts` | Cohort month/observation month | First observed positive-payment cohorts and month-end coverage |

## Private intermediate datasets

These six helpers are defined in `pipeline/transformations/gold.py` with `private=True` and support the public reports:

- `_registration_monthly`: shared registration counts.
- `_listening_monthly`: shared listening components.
- `_subscription_raw_events`: collapse exact business-value duplicates for the subscription model.
- `_subscription_events`: choose a canonical same-day subscription state.
- `_subscription_intervals`: derive observed membership coverage intervals.
- `_subscription_episodes`: join renewal coverage into retention episodes using a 30-day grace rule.

Lakeflow determines dependency order from the dataset references. The analysis, cleanup, and export notebooks are supporting assets, not pipeline source modules.

## Quality and interpretation

Silver preserves original field values and source rows. `dp.expect` records quality metrics without dropping records. Invalid parses become NULL where defined, and `quality_issues` records reasons. Member age is restricted to 1–100; zero becomes NULL. Listening duration outside 0–86,400 seconds becomes NULL while the original value and issue are retained.

Registration, city, and payment method codes are categorical identifiers; the source does not supply verified human-readable mappings. Cancellation events are not equivalent to churn labels.

The listening gold helper counts retained silver rows as listener-days and assumes the intended member/day grain; uniqueness must be checked separately. MAU counts distinct active members within a calendar month. Playtime uses active records with valid durations. Near-complete playback means the source bucket above 98.5%, rather than exactly 100%, and is not a satisfaction measure.

The subscription model collapses exact business-value duplicates only within gold. Its same-day ordering prioritizes cancellation state, then expiry and the recorded monetary/plan fields; this is a modelling heuristic rather than an ordered payment ledger. Renewal episodes bridge gaps within 30 days, but those gaps do not imply continuous paid coverage.

`subscriptions` estimates MRR from positive payments divided by plan duration and annualized to an average month; ARR is MRR × 12, ARPU is MRR divided by active payers, and LTV is ARPU divided by modelled churn rate. Churn, retention, and LTV require the 30-day observation window and otherwise remain NULL. These are observed-data estimates, not audited revenue or lifetime forecasts.

`cohorts` uses first observed positive payment. That payment may not be the member's first-ever payment. `source_coverage` reports observed bounds and counts, without proving the completeness of the downloaded source data.
