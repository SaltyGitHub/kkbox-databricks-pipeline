from pyspark import pipelines as dp
from pyspark.sql import functions as F


TX_COLUMNS = [
    "msno",
    "payment_method_id",
    "payment_plan_days",
    "plan_list_price",
    "actual_amount_paid",
    "is_auto_renew",
    "transaction_date",
    "membership_expire_date",
    "is_cancel"
]

TX_SCHEMA = """
    member_id STRING
        COMMENT 'Case-sensitive member identifier from msno. Not unique per transaction.',
    payment_method_code INT
        COMMENT 'KKBox payment method category code; method names are unknown.',
    subscription_plan_days INT
        COMMENT 'Subscription plan duration in days. Zero retained; negative values flagged.',
    plan_list_price_ntd DECIMAL(18,2)
        COMMENT 'Listed plan price in New Taiwan dollars. Zero and negative values retained.',
    amount_paid_ntd DECIMAL(18,2)
        COMMENT 'Recorded actual payment in New Taiwan dollars. Zero and negative values retained.',
    is_auto_renew BOOLEAN
        COMMENT 'Whether automatic renewal was enabled: source 1=true, 0=false.',
    transaction_date DATE
        COMMENT 'Transaction date parsed from yyyyMMdd.',
    membership_expiry_date DATE
        COMMENT 'Recorded membership expiration date parsed from yyyyMMdd.',
    is_cancellation BOOLEAN
        COMMENT 'Whether this transaction records a cancellation. This is not a churn label.',
    quality_issues ARRAY<STRING>
        COMMENT 'Missing, invalid or questionable values identified during cleanup.',
    source_table STRING
        COMMENT 'Fully qualified raw table this record came from.',
    source_values STRUCT<
        msno: STRING,
        payment_method_id: STRING,
        payment_plan_days: STRING,
        plan_list_price: STRING,
        actual_amount_paid: STRING,
        is_auto_renew: STRING,
        transaction_date: STRING,
        membership_expire_date: STRING,
        is_cancel: STRING
    > COMMENT 'Original source values represented as strings before cleanup.'
"""


def clean_transactions(source_table):
    def normalise(column):
        value = F.trim(F.col(column).cast("string"))
        return F.when(
            value.isNull()
            | F.lower(value).isin("", "null", "none", "nan"),
            F.lit(None).cast("string")
        ).otherwise(value)

    prepared = spark.read.table(source_table).select(
        *[normalise(c).alias(c) for c in TX_COLUMNS],
        F.struct(*[
            F.col(c).cast("string").alias(c)
            for c in TX_COLUMNS
        ]).alias("source_values")
    )

    # Reject fractional integers rather than silently truncating them.
    def integer(column):
        return F.when(
            F.col(column).rlike(r"^[+-]?[0-9]+$"),
            F.expr(f"try_cast(`{column}` AS INT)")
        )

    # Reject extra decimal places rather than silently rounding them.
    def money(column):
        return F.when(
            F.col(column).rlike(r"^[+-]?[0-9]+(\.[0-9]{1,2})?$"),
            F.expr(f"try_cast(`{column}` AS DECIMAL(18,2))")
        )

    def boolean(column):
        return (
            F.when(F.col(column) == "1", F.lit(True))
            .when(F.col(column) == "0", F.lit(False))
        )

    def date_value(column):
        return F.when(
            F.col(column).rlike(r"^[0-9]{8}$"),
            F.expr(
                f"CAST(try_to_timestamp(`{column}`, 'yyyyMMdd') AS DATE)"
            )
        )

    # Each definition maps a source field to its silver name and type.
    definitions = [
        ("msno", "member_id", F.col("msno")),
        ("payment_method_id", "payment_method_code",
         integer("payment_method_id")),
        ("payment_plan_days", "subscription_plan_days",
         integer("payment_plan_days")),
        ("plan_list_price", "plan_list_price_ntd",
         money("plan_list_price")),
        ("actual_amount_paid", "amount_paid_ntd",
         money("actual_amount_paid")),
        ("is_auto_renew", "is_auto_renew",
         boolean("is_auto_renew")),
        ("transaction_date", "transaction_date",
         date_value("transaction_date")),
        ("membership_expire_date", "membership_expiry_date",
         date_value("membership_expire_date")),
        ("is_cancel", "is_cancellation",
         boolean("is_cancel"))
    ]

    # Keep source text alongside parsed values while evaluating checks.
    parsed = prepared.select(
        "*",
        *[
            expression.alias(f"_parsed_{target}")
            for _, target, expression in definitions
        ]
    )

    issues = []

    for source, target, _ in definitions:
        issues.append(
            F.when(
                F.col(source).isNull(),
                F.lit(f"missing_{target}")
            )
        )
        issues.append(
            F.when(
                F.col(source).isNotNull()
                & F.col(f"_parsed_{target}").isNull(),
                F.lit(f"invalid_{target}")
            )
        )

    # Review flags: retain the parsed values rather than guessing fixes.
    for target in [
        "payment_method_code",
        "subscription_plan_days",
        "plan_list_price_ntd",
        "amount_paid_ntd"
    ]:
        issues.append(
            F.when(
                F.col(f"_parsed_{target}") < 0,
                F.lit(f"negative_{target}")
            )
        )

    issues.append(
        F.when(
            F.col("_parsed_membership_expiry_date")
            < F.col("_parsed_transaction_date"),
            F.lit("expiry_before_transaction")
        )
    )

    return parsed.select(
        *[
            F.col(f"_parsed_{target}").alias(target)
            for _, target, _ in definitions
        ],
        F.filter(
            F.array(*issues),
            lambda issue: issue.isNotNull()
        ).alias("quality_issues"),
        F.lit(source_table).alias("source_table"),
        "source_values"
    )


@dp.materialized_view(
    name="workspace.kkbox_silver.subscription_transactions",
    comment=(
        "Cleaned subscription transactions from kkbox_raw.transactions "
        "and kkbox_raw.transactions_v2. "
        "All source rows retained, including duplicates; source_table records provenance."
    ),
    schema=TX_SCHEMA
)
@dp.expect("member_id_present", "member_id IS NOT NULL")
@dp.expect("transaction_date_present", "transaction_date IS NOT NULL")
@dp.expect("no_cleanup_issues", "size(quality_issues) = 0")
def subscription_transactions():
    return clean_transactions("workspace.kkbox_raw.transactions").unionByName(
        clean_transactions("workspace.kkbox_raw.transactions_v2")
    )
