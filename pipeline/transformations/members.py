from pyspark import pipelines as dp
from pyspark.sql import functions as F

SOURCE = "workspace.kkbox_raw.members_v3"
MAX_AGE = 100  # Configurable project rule

SOURCE_COLUMNS = [
    "msno", "city", "bd", "gender",
    "registered_via", "registration_init_time"
]


def clean_text(column):
    value = F.trim(F.col(column).cast("string"))
    return F.when(
        value.isNull() | F.lower(value).isin("", "null", "none", "nan"),
        F.lit(None).cast("string")
    ).otherwise(value)


@dp.materialized_view(
    name="workspace.kkbox_silver.members",
    comment=(
        "Cleaned KKBox member records from members_v3. "
        "All source rows retained, including duplicates. "
        "Original field values retained for auditing."
    ),
    schema=f"""
        member_id STRING
            COMMENT 'Case-sensitive member identifier from msno; join key.',
        city_code INT
            COMMENT 'KKBox city category code; geographic labels unknown.',
        age INT
            COMMENT 'Reported age, not age at registration. Zero and values outside 1–{MAX_AGE} become NULL.',
        gender STRING
            COMMENT 'Normalised male or female; missing or unrecognised values become NULL.',
        registration_channel_code INT
            COMMENT 'Registration method category code from registered_via.',
        registration_date DATE
            COMMENT 'Registration date parsed using yyyyMMdd.',
        quality_issues ARRAY<STRING>
            COMMENT 'Cleanup issues; empty array means no listed checks failed.',
        source_values STRUCT<
            msno: STRING,
            city: STRING,
            bd: STRING,
            gender: STRING,
            registered_via: STRING,
            registration_init_time: STRING
        > COMMENT 'Original field values represented as strings before cleanup.'
    """
)
# Record quality metrics without dropping rows or stopping the pipeline.
@dp.expect("member_id_present", "member_id IS NOT NULL")
@dp.expect("registration_date_present", "registration_date IS NOT NULL")
@dp.expect("no_cleanup_issues", "size(quality_issues) = 0")
def members():
    raw = spark.read.table(SOURCE)

    prepared = raw.select(
        F.struct(*[
            F.col(c).cast("string").alias(c)
            for c in SOURCE_COLUMNS
        ]).alias("source_values"),
        clean_text("msno").alias("member_id"),
        clean_text("city").alias("_city"),
        clean_text("bd").alias("_age"),
        F.lower(clean_text("gender")).alias("_gender"),
        clean_text("registered_via").alias("_channel"),
        clean_text("registration_init_time").alias("_registration")
    )

    prepared = (
        prepared
        .withColumn("_city_int", F.expr("try_cast(_city AS INT)"))
        .withColumn("_age_int", F.expr("try_cast(_age AS INT)"))
        .withColumn("_channel_int", F.expr("try_cast(_channel AS INT)"))
        .withColumn(
            "_registration_date",
            F.when(
                F.col("_registration").rlike(r"^[0-9]{8}$"),
                F.expr(
                    "CAST(try_to_timestamp(_registration, 'yyyyMMdd') AS DATE)"
                )
            )
        )
    )

    checks = [
        (
            "missing_member_id",
            F.col("member_id").isNull()
        ),
        (
            "invalid_city_code",
            F.col("_city").isNotNull() & F.col("_city_int").isNull()
        ),
        (
            "invalid_age",
            F.col("_age").isNotNull()
            & (
                F.col("_age_int").isNull()
                | (
                    (F.col("_age_int") != 0)
                    & ~F.col("_age_int").between(1, MAX_AGE)
                )
            )
        ),
        (
            "unrecognised_gender",
            F.col("_gender").isNotNull()
            & ~F.col("_gender").isin("male", "female")
        ),
        (
            "invalid_registration_channel",
            F.col("_channel").isNotNull()
            & F.col("_channel_int").isNull()
        ),
        (
            "missing_registration_date",
            F.col("_registration").isNull()
        ),
        (
            "invalid_registration_date",
            F.col("_registration").isNotNull()
            & F.col("_registration_date").isNull()
        )
    ]

    return prepared.select(
        "member_id",
        F.col("_city_int").alias("city_code"),
        F.when(
            F.col("_age_int").between(1, MAX_AGE),
            F.col("_age_int")
        ).alias("age"),
        F.when(
            F.col("_gender").isin("male", "female"),
            F.col("_gender")
        ).alias("gender"),
        F.col("_channel_int").alias("registration_channel_code"),
        F.col("_registration_date").alias("registration_date"),
        F.filter(
            F.array(*[
                F.when(condition, F.lit(label))
                for label, condition in checks
            ]),
            lambda issue: issue.isNotNull()
        ).alias("quality_issues"),
        "source_values"
    )
