import re
from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col,
    lit,
    when,
    regexp_extract,
    concat,
    xxhash64,
    abs,
    coalesce,
    year,
    expr,
    regexp_replace,
)
from utils.utils import rename_column_to_snake_case, drop_null_rows, fill_unknown


@dp.table(
    name="marathos.silver.marathos_obt",
    comment="Cleaned data for Marathos",
    table_properties={
        "delta.columnMapping.mode": "name",
        "delta.minReaderVersion": "2",
        "delta.minWriterVersion": "5",
    },
)
def clean_marathos():
    # Read and union both race datasets before cleaning
    df_main = rename_column_to_snake_case(spark.sql("FROM marathos.bronze.raw_marathos"))
    df_stockholm = rename_column_to_snake_case(spark.sql("FROM marathos.bronze.stockholm_trail_classic_2024"))
    df = df_main.unionByName(df_stockholm, allowMissingColumns=True)

    # Rename all columns to snake_case
    df = rename_column_to_snake_case(df)

    # Replace nulls with "Unknown" for non-critical string columns
    df = fill_unknown(df, ["athlete_club", "athlete_country"])

    df = (
        df
        # --- Validate event/performance unit consistency ---
        # km/mi/mile events should have time (h) as performance
        # h events should have distance (km) as performance
        # d (days) events are always invalid
        .withColumn(
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
        # --- Parse event dates ---
        # Extract start date from formats like "21.-22.04.2018" or "17.06.2018"
        # try_to_date returns null for invalid dates (e.g. 31.04.2018) instead of crashing
        .withColumn(
            "event_start_date",
            expr(
                "try_to_date(concat(regexp_extract(event_dates, '^(\\\\d{2})', 1), '.', "
                "regexp_extract(event_dates, '(\\\\d{2}\\\\.\\\\d{4})', 1)), 'dd.MM.yyyy')"
            ),
        )
        # Extract end date from the last full date in the string
        .withColumn(
            "event_end_date",
            expr(
                "try_to_date(regexp_extract(event_dates, '(\\\\d{2}\\\\.\\\\d{2}\\\\.\\\\d{4})$', 1), 'dd.MM.yyyy')"
            ),
        )
        # Drop rows with invalid or missing start dates
        .filter(col("event_start_date").isNotNull())
        # --- Drop rows where critical fields are null ---
        .transform(
            lambda df: drop_null_rows(
                df,
                [
                    "athlete_performance",
                    "athlete_id",
                    "athlete_gender",
                    "athlete_age_category",
                    "event_distance/length",
                ],
            )
        )
        # --- Convert performance string (e.g. "6:21:03 h") to total seconds ---
        .withColumn(
            "performance_seconds",
            when(
                col("athlete_performance").rlike(r"\d+:\d+:\d+"),
                regexp_extract(col("athlete_performance"), r"(\d+):(\d+):(\d+)", 1).cast("int") * 3600
                + regexp_extract(col("athlete_performance"), r"(\d+):(\d+):(\d+)", 2).cast("int") * 60
                + regexp_extract(col("athlete_performance"), r"(\d+):(\d+):(\d+)", 3).cast("int"),
            ).otherwise(None),
        )
        # --- Cast average speed to double, handle malformed values (e.g. "18:00:00") ---
        .withColumn(
            "athlete_average_speed",
            expr("try_cast(athlete_average_speed as double)"),
        )
        # Filter out unrealistic speeds (world record ~20.8 km/h, minimum 2.0 km/h)
        .filter(
            col("athlete_average_speed").isNull()
            | (
                (col("athlete_average_speed") <= 20.8)
                & (col("athlete_average_speed") >= 2.0)
            )
        )
        # Replace null speeds with 0.0
        .withColumn(
            "athlete_average_speed",
            coalesce(col("athlete_average_speed"), lit(0.0)),
        )
        # --- Cast year of birth to integer ---
        .withColumn(
            "athlete_year_of_birth",
            col("athlete_year_of_birth").cast("integer"),
        )
        # Filter out athletes younger than 18 or older than 100 at time of event
        .filter(
            (year(col("event_start_date")) - col("athlete_year_of_birth") >= 18)
            & (year(col("event_start_date")) - col("athlete_year_of_birth") <= 100)
        )
        # --- Normalize age category: replace "F" prefix with "W" for consistency ---
        .withColumn(
            "athlete_age_category",
            regexp_replace(col("athlete_age_category"), "^F", "W"),
        )
        # --- Generate IDs using hashing ---
        # event_id: unique per event name
        # result_id: unique per athlete + event combination
        .withColumn("event_id", abs(xxhash64(col("event_name"))))
        .withColumn("result_id", abs(xxhash64(col("event_name"), col("athlete_id"))))
    )

    # Load country codes and join - inner join drops rows with invalid or missing country codes
    country_codes = spark.sql("FROM marathos.bronze.country_codes")
    df = df.join(country_codes, on="athlete_country", how="inner")

    return df