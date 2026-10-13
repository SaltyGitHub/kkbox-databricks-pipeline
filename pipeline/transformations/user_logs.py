from pyspark import pipelines as dp
from pyspark.sql import functions as F


LOG_COLUMNS = [
    "msno", "date", "num_25", "num_50", "num_75",
    "num_985", "num_100", "num_unq", "total_secs"
]

COUNT_COLUMNS = {
    "num_25": "plays_under_25_pct",
    "num_50": "plays_25_to_50_pct",
    "num_75": "plays_50_to_75_pct",
    "num_985": "plays_75_to_98_5_pct",
    "num_100": "plays_over_98_5_pct",
    "num_unq": "unique_songs_played"
}

LOG_SCHEMA = """
    member_id STRING
        COMMENT 'Case-sensitive member identifier from msno; join key.',
    activity_date DATE
        COMMENT 'Listening activity date parsed from yyyyMMdd.',
    plays_under_25_pct BIGINT
        COMMENT 'Playback count in the source bucket below 25 percent completion.',
    plays_25_to_50_pct BIGINT
        COMMENT 'Playback count in the source 25 to 50 percent completion bucket.',
    plays_50_to_75_pct BIGINT
        COMMENT 'Playback count in the source 50 to 75 percent completion bucket.',
    plays_75_to_98_5_pct BIGINT
        COMMENT 'Playback count in the source 75 to 98.5 percent completion bucket.',
    plays_over_98_5_pct BIGINT
        COMMENT 'Playback count above 98.5 percent completion; not necessarily exactly 100 percent.',
    unique_songs_played BIGINT
        COMMENT 'Number of distinct songs played in this source activity record.',
    listening_seconds DECIMAL(24,4)
        COMMENT 'Daily listening seconds in the project range 0 to 86400 inclusive; otherwise NULL. Original duration retained in source_values.total_secs and reason retained in quality_issues.',
    quality_issues ARRAY<STRING>
        COMMENT 'Missing, invalid or questionable values identified during cleanup.',
    source_table STRING
        COMMENT 'Fully qualified raw table this record came from.',
    source_values STRUCT<
        msno: STRING,
        date: STRING,
        num_25: STRING,
        num_50: STRING,
        num_75: STRING,
        num_985: STRING,
        num_100: STRING,
        num_unq: STRING,
        total_secs: STRING
    > COMMENT 'Original source values represented as strings before cleanup.'
"""


def clean_listening_activity(source_table):
    def normalise(column):
        value = F.trim(F.col(column).cast("string"))
        return F.when(
            value.isNull()
            | F.lower(value).isin("", "null", "none", "nan"),
            F.lit(None).cast("string")
        ).otherwise(value)

    prepared = spark.read.table(source_table).select(
        *[normalise(c).alias(c) for c in LOG_COLUMNS],
        F.struct(*[
            F.col(c).cast("string").alias(c)
            for c in LOG_COLUMNS
        ]).alias("source_values")
    )

    def whole_number(column):
        # Reject fractional counts rather than silently truncating them.
        return F.when(
            F.col(column).rlike(r"^[+-]?[0-9]+$"),
            F.expr(f"try_cast(`{column}` AS BIGINT)")
        )

    activity_date = F.when(
        F.col("date").rlike(r"^[0-9]{8}$"),
        F.expr(
            "CAST(try_to_timestamp(`date`, 'yyyyMMdd') AS DATE)"
        )
    )

    # Reject additional decimal places rather than silently rounding.
    listening_seconds = F.when(
        F.col("total_secs").rlike(r"^[+-]?[0-9]+(\.[0-9]{1,4})?$"),
        F.expr("try_cast(total_secs AS DECIMAL(24,4))")
    )

    definitions = [
        ("msno", "member_id", F.col("msno")),
        ("date", "activity_date", activity_date),
        *[
            (source, target, whole_number(source))
            for source, target in COUNT_COLUMNS.items()
        ],
        ("total_secs", "listening_seconds", listening_seconds)
    ]

    parsed = prepared.select(
        "*",
        *[
            expression.alias(f"_parsed_{target}")
            for _, target, expression in definitions
        ]
    )

    issues = []

    for source, target, _ in definitions:
        issues.extend([
            F.when(
                F.col(source).isNull(),
                F.lit(f"missing_{target}")
            ),
            F.when(
                F.col(source).isNotNull()
                & F.col(f"_parsed_{target}").isNull(),
                F.lit(f"invalid_{target}")
            )
        ])

    # Evaluate warnings on parsed source values before cleaning the duration.
    for target in [*COUNT_COLUMNS.values(), "listening_seconds"]:
        issues.append(
            F.when(
                F.col(f"_parsed_{target}") < 0,
                F.lit(f"negative_{target}")
            )
        )

    # Retain the reason and original value when a duration exceeds the project range.
    issues.append(
        F.when(
            F.col("_parsed_listening_seconds") > 86400,
            F.lit("listening_exceeds_24_hours")
        )
    )

    # Approved project rule: null only the duration; keep the row and all other values.
    return parsed.select(
        *[
            (
                F.when(
                    F.col(f"_parsed_{target}").between(0, 86400),
                    F.col(f"_parsed_{target}")
                )
                if target == "listening_seconds"
                else F.col(f"_parsed_{target}")
            ).alias(target)
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
    name="workspace.kkbox_silver.daily_listening_activity",
    comment=(
        "Cleaned listening activity from kkbox_raw.user_logs "
        "and kkbox_raw.user_logs_v2. "
        "All source rows retained; source_table records provenance. "
        "Durations outside 0 to 86400 seconds become NULL; originals and warnings retained. "
        "Member and activity date form the logical grain; uniqueness is checked separately."
    ),
    schema=LOG_SCHEMA
)
@dp.expect("member_id_present", "member_id IS NOT NULL")
@dp.expect("activity_date_present", "activity_date IS NOT NULL")
@dp.expect("no_cleanup_issues", "size(quality_issues) = 0")
def daily_listening_activity():
    return clean_listening_activity("workspace.kkbox_raw.user_logs").unionByName(
        clean_listening_activity("workspace.kkbox_raw.user_logs_v2")
    )
