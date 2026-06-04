import re
from pyspark.sql.functions import (
    col,
    lit,
    when,
    regexp_extract,
    regexp_replace,
    xxhash64,
    abs,
    coalesce,
    year,
    expr,
)

# ---------------------------------------------------------------------------
# General helpers
# ---------------------------------------------------------------------------


def to_snake_case(name):
    """Converts a column name to snake_case."""
    return re.sub(r"[\s]+", "_", name.strip().casefold())


def rename_column_to_snake_case(df):
    """Renames all columns in a DataFrame to snake_case."""
    new_columns = [to_snake_case(column) for column in df.columns]
    return df.toDF(*new_columns)


def drop_null_rows(df, columns):
    """Drops rows where any of the specified columns contain null values."""
    for c in columns:
        df = df.filter(col(c).isNotNull())
    return df


def fill_unknown(df, columns):
    """Replaces null values in specified string columns with 'Unknown'."""
    for c in columns:
        df = df.withColumn(c, coalesce(col(c).cast("string"), lit("Unknown")))
    return df


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


def validate_performance_units(df):
    """
    Validates that event distance/length unit matches performance unit.
    - km/mi/mile events must have time (h) as performance
    - h events must have distance (km) as performance
    - d (days) events are always invalid and removed
    Rows that fail validation are dropped.
    """
    return (
        df.withColumn(
            "is_valid",
            when(
                col("event_distance/length").rlike("km|mi|mile"),
                col("athlete_performance").endswith("h"),
            )
            .when(
                col("event_distance/length").rlike("h"),
                col("athlete_performance").endswith("km"),
            )
            .when(col("event_distance/length").endswith("d"), False)
            .otherwise(False),
        )
        .filter(col("is_valid") == True)
        .drop("is_valid")
    )


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------


def parse_event_type(df):
    """
    Derives event_type from event_distance/length:
    - 'distance' for km/mi/mile events
    - 'time' for h events
    """
    return df.withColumn(
        "event_type",
        when(col("event_distance/length").rlike("km|mi|mile"), lit("distance"))
        .when(col("event_distance/length").rlike("h"), lit("time"))
        .otherwise(None),
    )

# Help from LLM - notis that my generated dataset had wrong date so it way fiilterd out
def parse_event_dates(df):
    """
    Parses event_dates string into event_start_date and event_end_date.
    Handles formats like '21.-22.04.2018' or '17.06.2018'.
    Uses try_to_date to return null for invalid dates instead of crashing.
    Rows with null start dates are dropped.
    """
    return (
        df.withColumn(
            "event_start_date",
            coalesce(
                # Format 1: europeiskt format "21.-22.04.2018" eller "17.06.2018"
                expr(
                    "try_to_date(concat(regexp_extract(event_dates, '^(\\\\d{2})', 1), '.', "
                    "regexp_extract(event_dates, '(\\\\d{2}\\\\.\\\\d{4})', 1)), 'dd.MM.yyyy')"
                ),
                # Format 2: ISO-format "2024-06-15"
                expr("try_to_date(event_dates, 'yyyy-MM-dd')"),
                # Format 3: redan ett datum (från Stockholm-datan)
                col("event_start_date"),
            ),
        )
        .withColumn(
            "event_end_date",
            coalesce(
                expr(
                    "try_to_date(regexp_extract(event_dates, '(\\\\d{2}\\\\.\\\\d{2}\\\\.\\\\d{4})$', 1), 'dd.MM.yyyy')"
                ),
                expr("try_to_date(event_dates, 'yyyy-MM-dd')"),
                col("event_end_date"),
            ),
        )
        .filter(col("event_start_date").isNotNull())
    )


def parse_performance(df):
    """
    Parses athlete_performance string into numeric columns:
    - performance_seconds: for distance events (e.g. '6:21:03 h' -> 22863)
    - performance_km: for time events (e.g. '123.5 km' -> 123.5)
    Rows with performance_seconds <= 1h and no valid km value are dropped.
    """
    return (
        df.withColumn(
            "performance_seconds",
            when(
                col("athlete_performance").rlike(r"\d+:\d+:\d+"),
                regexp_extract(
                    col("athlete_performance"), r"(\d+):(\d+):(\d+)", 1
                ).cast("int")
                * 3600
                + regexp_extract(
                    col("athlete_performance"), r"(\d+):(\d+):(\d+)", 2
                ).cast("int")
                * 60
                + regexp_extract(
                    col("athlete_performance"), r"(\d+):(\d+):(\d+)", 3
                ).cast("int"),
            ).otherwise(None),
        )
        .filter(
            (col("performance_seconds") > 3600)  # at least 1 hour
            | col("athlete_performance").rlike(r"\d+\.?\d*\s*km")
        )
        .withColumn(
            "performance_km",
            when(
                col("athlete_performance").rlike(r"\d+\.?\d*\s*km"),
                regexp_extract(col("athlete_performance"), r"(\d+\.?\d*)", 1).cast(
                    "double"
                ),
            ).otherwise(None),
        )
        .filter((col("performance_seconds") > 3600) | (col("performance_km") > 0))
    )


# ---------------------------------------------------------------------------
# Filtering
# ---------------------------------------------------------------------------


def filter_realistic_performance(df):
    """
    Removes statistically unrealistic performances per event type:
    - Distance events: 1h–60h (3 600–216 000 seconds)
    - Time events: 1–500 km
    Rows outside these bounds are considered data quality issues and dropped.
    """
    return (
        df.withColumn(
            "is_realistic_performance",
            when(
                col("event_type") == "distance",
                col("performance_seconds").between(3600, 216000),
            )
            .when(col("event_type") == "time", col("performance_km").between(1, 500))
            .otherwise(False),
        )
        .filter(col("is_realistic_performance") == True)
        .drop("is_realistic_performance")
    )


def filter_athlete_speed(df):
    """
    Casts athlete_average_speed to double and removes unrealistic values.
    Valid range: 2.0–20.8 km/h (world record ~20.8 km/h, minimum walking pace).
    Rows with null speed are kept; null is replaced with 0.0 afterwards.
    """
    return (
        df.withColumn(
            "athlete_average_speed",
            expr("try_cast(athlete_average_speed as double)"),
        )
        .filter(
            col("athlete_average_speed").isNull()
            | (
                (col("athlete_average_speed") <= 20.8)
                & (col("athlete_average_speed") >= 2.0)
            )
        )
        .withColumn(
            "athlete_average_speed",
            coalesce(col("athlete_average_speed"), lit(0.0)),
        )
    )


def filter_athlete_age(df):
    """
    Removes athletes who were younger than 18 or older than 100
    at the time of the event, based on year of birth and event start date.
    """
    return df.withColumn(
        "athlete_year_of_birth",
        col("athlete_year_of_birth").cast("integer"),
    ).filter(
        (year(col("event_start_date")) - col("athlete_year_of_birth") >= 18)
        & (year(col("event_start_date")) - col("athlete_year_of_birth") <= 100)
    )


# ---------------------------------------------------------------------------
# Enrichment
# ---------------------------------------------------------------------------


def normalize_age_category(df):
    """
    Normalizes athlete_age_category by replacing legacy 'F' prefix with 'W'
    (e.g. 'F40' -> 'W40') for consistency across datasets.
    """
    return df.withColumn(
        "athlete_age_category",
        regexp_replace(col("athlete_age_category"), "^F", "W"),
    )


def generate_ids(df):
    """
    Generates surrogate keys using xxhash64:
    - event_id: unique per event name
    - result_id: unique per athlete + event combination
    abs() ensures positive values.
    """
    return df.withColumn("event_id", abs(xxhash64(col("event_name")))).withColumn(
        "result_id", abs(xxhash64(col("event_name"), col("athlete_id")))
    )


def clean_club_names(df):
    """Removes leading asterisks from athlete_club values."""
    return df.withColumn(
        "athlete_club",
        regexp_replace(col("athlete_club"), r"^\*", ""),
    )
