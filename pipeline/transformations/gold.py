from pyspark import pipelines as dp

# Monthly reporting gold for the existing KKBOX dashboard.
# Silver remains the detailed, cleaned source. Dashboard month selection is downstream.
# Private helpers share heavy calculations and are not published as business tables.

@dp.materialized_view(
    name="workspace.kkbox_gold.source_coverage",
    comment='Audit support: observed date ranges and retained row counts in silver. Not a business entity or proof of complete source coverage.',
    table_properties={"quality": "gold"}
)
def goldsource_coverage():
    return spark.sql("""
SELECT 'members' source, min(registration_date) min_date,
       max(registration_date) max_date, count(*) row_count
FROM workspace.kkbox_silver.members
UNION ALL
SELECT 'listening', min(activity_date), max(activity_date), count(*)
FROM workspace.kkbox_silver.daily_listening_activity
UNION ALL
SELECT 'transactions', min(transaction_date), max(transaction_date), count(*)
FROM workspace.kkbox_silver.subscription_transactions
""")


@dp.materialized_view(
    name="workspace.kkbox_gold._registration_monthly",
    comment='Shared monthly registration counts by observed registration method code.',
    table_properties={"quality": "gold"},
    private=True
)
def gold_registration_monthly():
    return spark.sql("""
SELECT CAST(date_trunc('month', registration_date) AS DATE) month,
       coalesce(CAST(registration_channel_code AS STRING), 'Unknown') channel,
       count(*) registrations
FROM workspace.kkbox_silver.members
WHERE registration_date IS NOT NULL
GROUP BY 1, 2
""")


@dp.materialized_view(
    name="workspace.kkbox_gold.signups",
    comment='Monthly observed account registrations. Logical key: month. Reporting aggregate, not a Customer entity.',
    table_properties={"quality": "gold"}
)
def goldsignups():
    return spark.sql("""
WITH bounds AS (
  SELECT CAST(date_trunc('month', min_date) AS DATE) first_month,
         CAST(date_trunc('month', max_date) AS DATE) last_month
  FROM workspace.kkbox_gold.source_coverage WHERE source = 'members'
), months AS (
  SELECT explode(sequence(first_month, last_month, INTERVAL 1 MONTH)) month
  FROM bounds
), counts AS (
  SELECT month, sum(registrations) signups
  FROM workspace.kkbox_gold._registration_monthly GROUP BY month
)
SELECT m.month, coalesce(c.signups, 0) signups
FROM months m LEFT JOIN counts c USING (month)
""")


@dp.materialized_view(
    name="workspace.kkbox_gold.registration_channels",
    comment='Monthly registrations by registration method code. Logical key: month, channel. Codes are not named marketing acquisition channels.',
    table_properties={"quality": "gold"}
)
def goldregistration_channels():
    return spark.sql("""
SELECT month, channel, registrations
FROM workspace.kkbox_gold._registration_monthly
""")


@dp.materialized_view(
    name="workspace.kkbox_gold._listening_monthly",
    comment='Shared monthly listening components; one unique silver member/day. MAU is calendar-month distinct active listeners.',
    table_properties={"quality": "gold"},
    private=True
)
def gold_listening_monthly():
    return spark.sql("""
WITH prepared AS (
  SELECT *,
    (CAST(coalesce(plays_under_25_pct, 0) AS BIGINT)
      + coalesce(plays_25_to_50_pct, 0)
      + coalesce(plays_50_to_75_pct, 0)
      + coalesce(plays_75_to_98_5_pct, 0)
      + coalesce(plays_over_98_5_pct, 0)) total_plays,
    (plays_under_25_pct IS NOT NULL AND plays_25_to_50_pct IS NOT NULL
      AND plays_50_to_75_pct IS NOT NULL AND plays_75_to_98_5_pct IS NOT NULL
      AND plays_over_98_5_pct IS NOT NULL
      AND least(plays_under_25_pct, plays_25_to_50_pct, plays_50_to_75_pct,
                plays_75_to_98_5_pct, plays_over_98_5_pct) >= 0) valid_buckets
  FROM workspace.kkbox_silver.daily_listening_activity
  WHERE member_id IS NOT NULL AND activity_date IS NOT NULL
), activity AS (
  SELECT *, (total_plays > 0 OR listening_seconds > 0) is_active
  FROM prepared
)
SELECT CAST(date_trunc('month', activity_date) AS DATE) month,
       count(*) listener_days,
       count_if(is_active) active_listener_days,
       count(DISTINCT CASE WHEN is_active THEN member_id END) mau,
       sum(CASE WHEN is_active AND listening_seconds IS NOT NULL
                THEN listening_seconds END) valid_seconds,
       count_if(is_active AND listening_seconds IS NOT NULL) valid_listener_days,
       count_if(valid_buckets) valid_bucket_days,
       sum(CASE WHEN valid_buckets THEN total_plays END) valid_total_plays,
       sum(CASE WHEN valid_buckets THEN plays_under_25_pct END) b1,
       sum(CASE WHEN valid_buckets THEN plays_25_to_50_pct END) b2,
       sum(CASE WHEN valid_buckets THEN plays_50_to_75_pct END) b3,
       sum(CASE WHEN valid_buckets THEN plays_75_to_98_5_pct END) b4,
       sum(CASE WHEN valid_buckets THEN plays_over_98_5_pct END) b5
FROM activity GROUP BY 1
""")


@dp.materialized_view(
    name="workspace.kkbox_gold.engagement",
    comment='Monthly listening engagement. Logical key: month. MAU is calendar-month active listeners; playtime is per active member/day with valid duration.',
    table_properties={"quality": "gold"}
)
def goldengagement():
    return spark.sql("""
SELECT month, active_listener_days, mau, valid_seconds, valid_listener_days,
       active_listener_days / day(last_day(month)) avg_dau,
       try_divide(active_listener_days / day(last_day(month)), mau) * 100 stickiness_pct,
       try_divide(valid_seconds, 60 * valid_listener_days) playtime_minutes,
       try_divide(valid_listener_days, active_listener_days) * 100 duration_coverage_pct
FROM workspace.kkbox_gold._listening_monthly
""")


@dp.materialized_view(
    name="workspace.kkbox_gold.listening_quality",
    comment='Monthly play-weighted completion signals and bucket coverage. Logical key: month. These are behavior measures, not satisfaction scores.',
    table_properties={"quality": "gold"}
)
def goldlistening_quality():
    return spark.sql("""
SELECT month, listener_days, valid_bucket_days, valid_total_plays,
       b5 near_complete_plays, b1 early_stop_plays,
       try_divide(b5, valid_total_plays) * 100 near_complete_pct,
       try_divide(b1, valid_total_plays) * 100 early_stop_pct,
       try_divide(valid_bucket_days, listener_days) * 100 bucket_coverage_pct
FROM workspace.kkbox_gold._listening_monthly
""")


@dp.materialized_view(
    name="workspace.kkbox_gold.completion_mix",
    comment='Monthly share of plays in each completion bucket. Logical key: month, bucket. Retains counts for weighted calculations.',
    table_properties={"quality": "gold"}
)
def goldcompletion_mix():
    return spark.sql("""
SELECT month, bucket, plays, valid_total_plays total_plays,
       try_divide(plays, valid_total_plays) * 100 share_pct
FROM workspace.kkbox_gold._listening_monthly
LATERAL VIEW stack(5,
  '1. Under 25%', b1, '2. 25–50%', b2, '3. 50–75%', b3,
  '4. 75–98.5%', b4, '5. Over 98.5%', b5) AS bucket, plays
""")


@dp.materialized_view(
    name="workspace.kkbox_gold._subscription_raw_events",
    comment='Exact business-value duplicates collapsed only for subscription state modelling; silver remains unchanged.',
    table_properties={"quality": "gold"},
    private=True
)
def gold_subscription_raw_events():
    return spark.sql("""
SELECT DISTINCT
  member_id,
  transaction_date,
  membership_expiry_date,
  is_cancellation,
  subscription_plan_days,
  amount_paid_ntd,
  plan_list_price_ntd,
  payment_method_code,
  is_auto_renew
FROM workspace.kkbox_silver.subscription_transactions
WHERE member_id IS NOT NULL
  AND transaction_date IS NOT NULL
  AND membership_expiry_date IS NOT NULL
""")


@dp.materialized_view(
    name="workspace.kkbox_gold._subscription_events",
    comment='Canonical daily subscription state, using documented cancellation-first same-day heuristic. Not an ordered payment ledger.',
    table_properties={"quality": "gold"},
    private=True
)
def gold_subscription_events():
    return spark.sql("""
WITH daily_ranked AS (
  SELECT *,
    row_number() OVER (
      PARTITION BY member_id, transaction_date
      ORDER BY
        coalesce(is_cancellation, false) DESC,
        membership_expiry_date DESC,
        coalesce(amount_paid_ntd, 0) DESC,
        coalesce(subscription_plan_days, 0) DESC,
        coalesce(plan_list_price_ntd, 0) DESC,
        coalesce(payment_method_code, -1) DESC,
        coalesce(is_auto_renew, false) DESC
    ) AS rn
  FROM workspace.kkbox_gold._subscription_raw_events
)
SELECT * EXCEPT (rn)
FROM daily_ranked
WHERE rn = 1
""")


@dp.materialized_view(
    name="workspace.kkbox_gold._subscription_intervals",
    comment='Derived observed membership coverage intervals; latest daily state governs coverage. Impossible intervals excluded from this model.',
    table_properties={"quality": "gold"},
    private=True
)
def gold_subscription_intervals():
    return spark.sql("""
WITH ordered_events AS (
  SELECT *,
    lead(transaction_date) OVER (
      PARTITION BY member_id ORDER BY transaction_date
    ) AS next_event
  FROM workspace.kkbox_gold._subscription_events
), intervals AS (
  SELECT
    member_id,
    transaction_date AS start_date,
    least(
      membership_expiry_date,
      coalesce(date_sub(next_event, 1), membership_expiry_date)
    ) AS end_date
  FROM ordered_events
)
SELECT member_id, start_date, end_date
FROM intervals
WHERE end_date >= start_date
""")


@dp.materialized_view(
    name="workspace.kkbox_gold._subscription_episodes",
    comment='Derived retention episodes joining renewals within 30 days; grace gaps do not mean continuous paid coverage.',
    table_properties={"quality": "gold"},
    private=True
)
def gold_subscription_episodes():
    return spark.sql("""
WITH prior_coverage AS (
  SELECT *,
    max(end_date) OVER (
      PARTITION BY member_id ORDER BY start_date
      ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
    ) AS prior_end
  FROM workspace.kkbox_gold._subscription_intervals
), episode_flags AS (
  SELECT *,
    CASE WHEN prior_end IS NULL OR datediff(start_date, prior_end) > 30
      THEN 1 ELSE 0 END AS new_episode
  FROM prior_coverage
), episode_groups AS (
  SELECT *,
    sum(new_episode) OVER (
      PARTITION BY member_id ORDER BY start_date
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS episode_id
  FROM episode_flags
)
SELECT
  member_id,
  episode_id,
  min(start_date) AS episode_start,
  max(end_date) AS episode_end
FROM episode_groups
GROUP BY member_id, episode_id
""")


@dp.materialized_view(
    name="workspace.kkbox_gold.subscriptions",
    comment='Monthly modelled subscription retention and monetization estimates. Logical key: month. Unmatured churn, retention and LTV are NULL; MRR/ARR/ARPU are estimated.',
    table_properties={"quality": "gold"}
)
def goldsubscriptions():
    return spark.sql("""
WITH bounds AS (
  SELECT
    CASE WHEN min_date = CAST(date_trunc('month', min_date) AS DATE)
      THEN CAST(date_trunc('month', min_date) AS DATE)
      ELSE add_months(CAST(date_trunc('month', min_date) AS DATE), 1)
    END AS first_month,
    CASE WHEN max_date = last_day(max_date)
      THEN CAST(date_trunc('month', max_date) AS DATE)
      ELSE add_months(CAST(date_trunc('month', max_date) AS DATE), -1)
    END AS last_month,
    max_date AS observed_through
  FROM workspace.kkbox_gold.source_coverage
  WHERE source = 'transactions'
), months AS (
  SELECT explode(sequence(first_month, last_month, INTERVAL 1 MONTH)) AS month,
    observed_through
  FROM bounds
  WHERE first_month <= last_month
), start_members AS (
  SELECT DISTINCT m.month, i.member_id
  FROM months m
  JOIN workspace.kkbox_gold._subscription_intervals i
    ON i.start_date <= m.month AND i.end_date >= m.month
), churn_counts AS (
  SELECT s.month,
    count(*) AS start_active,
    count_if(e.episode_end BETWEEN s.month AND last_day(s.month)) AS churned
  FROM start_members s
  JOIN workspace.kkbox_gold._subscription_episodes e
    ON e.member_id = s.member_id
    AND e.episode_start <= s.month AND e.episode_end >= s.month
  GROUP BY s.month
), snapshot AS (
  SELECT
    m.month,
    e.member_id,
    max(named_struct(
      'event_date', e.transaction_date,
      'expiry', e.membership_expiry_date
    )) AS state,
    max(CASE
      WHEN e.amount_paid_ntd > 0 AND e.subscription_plan_days > 0
        AND NOT coalesce(e.is_cancellation, false)
      THEN named_struct(
        'paid_date', e.transaction_date,
        'rate', e.amount_paid_ntd / e.subscription_plan_days * 365.25 / 12
      )
    END) AS paid
  FROM months m
  JOIN workspace.kkbox_gold._subscription_events e
    ON e.transaction_date <= last_day(m.month)
  GROUP BY m.month, e.member_id
), money AS (
  SELECT month,
    count_if(state.expiry >= last_day(month) AND paid.rate IS NOT NULL) AS paying_active,
    sum(CASE WHEN state.expiry >= last_day(month) THEN paid.rate END) AS mrr
  FROM snapshot
  GROUP BY month
), metrics AS (
  SELECT
    m.month,
    last_day(m.month) AS observation_end,
    m.observed_through,
    date_add(last_day(m.month), 30) <= m.observed_through AS churn_observable,
    coalesce(c.start_active, 0) AS start_active,
    CASE WHEN date_add(last_day(m.month), 30) <= m.observed_through
      THEN coalesce(c.churned, 0) END AS churned,
    CASE WHEN date_add(last_day(m.month), 30) <= m.observed_through
      THEN try_divide(c.churned, c.start_active) * 100 END AS churn_pct,
    CASE WHEN date_add(last_day(m.month), 30) <= m.observed_through
      THEN (1 - try_divide(c.churned, c.start_active)) * 100 END AS retention_pct,
    coalesce(n.paying_active, 0) AS paying_active,
    n.mrr,
    n.mrr * 12 AS arr,
    try_divide(n.mrr, n.paying_active) AS arpu
  FROM months m
  LEFT JOIN churn_counts c USING (month)
  LEFT JOIN money n USING (month)
)
SELECT *, try_divide(arpu, churn_pct / 100) AS ltv
FROM metrics
""")


@dp.materialized_view(
    name="workspace.kkbox_gold.cohorts",
    comment='First observed positive-payment cohorts with observed month-end membership coverage. Logical key: cohort_month, observation_month. First observed payment is not necessarily first-ever payment.',
    table_properties={"quality": "gold"}
)
def goldcohorts():
    return spark.sql("""
WITH bounds AS (
  SELECT
    CASE WHEN min_date = CAST(date_trunc('month', min_date) AS DATE)
      THEN CAST(date_trunc('month', min_date) AS DATE)
      ELSE add_months(CAST(date_trunc('month', min_date) AS DATE), 1)
    END AS first_month,
    CASE WHEN max_date = last_day(max_date)
      THEN CAST(date_trunc('month', max_date) AS DATE)
      ELSE add_months(CAST(date_trunc('month', max_date) AS DATE), -1)
    END AS last_month,
    max_date AS observed_through
  FROM workspace.kkbox_gold.source_coverage
  WHERE source = 'transactions'
), first_paid AS (
  SELECT member_id,
    CAST(date_trunc('month', min(transaction_date)) AS DATE) AS cohort_month
  FROM workspace.kkbox_gold._subscription_raw_events
  WHERE amount_paid_ntd > 0 AND subscription_plan_days > 0
    AND NOT coalesce(is_cancellation, false)
  GROUP BY member_id
), observed_cohorts AS (
  SELECT f.*
  FROM first_paid f CROSS JOIN bounds b
  WHERE f.cohort_month BETWEEN b.first_month AND b.last_month
), cohort_sizes AS (
  SELECT cohort_month, count(*) AS cohort_size
  FROM observed_cohorts
  GROUP BY cohort_month
), cohort_grid AS (
  SELECT c.cohort_month, c.cohort_size, b.observed_through,
    explode(sequence(c.cohort_month, b.last_month, INTERVAL 1 MONTH)) AS observation_month
  FROM cohort_sizes c CROSS JOIN bounds b
), active_members AS (
  SELECT DISTINCT f.cohort_month, g.observation_month, f.member_id
  FROM observed_cohorts f
  JOIN cohort_grid g ON f.cohort_month = g.cohort_month
  JOIN workspace.kkbox_gold._subscription_intervals i
    ON i.member_id = f.member_id
    AND i.start_date <= last_day(g.observation_month)
    AND i.end_date >= last_day(g.observation_month)
), active_counts AS (
  SELECT cohort_month, observation_month, count(*) AS active_count
  FROM active_members
  GROUP BY cohort_month, observation_month
)
SELECT
  g.cohort_month,
  g.observation_month,
  CAST(months_between(g.observation_month, g.cohort_month) AS INT) AS month_index,
  date_format(g.cohort_month, 'yyyy-MM') AS cohort,
  concat('Month ', CAST(CAST(months_between(g.observation_month, g.cohort_month) AS INT) AS STRING))
    AS months_since_first_paid,
  g.cohort_size,
  coalesce(a.active_count, 0) AS active_count,
  try_divide(coalesce(a.active_count, 0), g.cohort_size) * 100 AS retention_pct,
  g.observed_through
FROM cohort_grid g
LEFT JOIN active_counts a USING (cohort_month, observation_month)
""")
