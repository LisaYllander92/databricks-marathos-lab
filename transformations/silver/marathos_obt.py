from pyspark import pipelines as dp
from utils.utils import (
    rename_column_to_snake_case,
    drop_null_rows,
    fill_unknown,
    validate_performance_units,
    parse_event_type,
    parse_event_dates,
    parse_performance,
    filter_realistic_performance,
    filter_athlete_speed,
    filter_athlete_age,
    normalize_age_category,
    generate_ids,
    clean_club_names,
)


@dp.table(
    name="marathos.silver.marathos_obt",
    comment="Cleaned and validated one big table for Marathos",
    table_properties={
        "delta.columnMapping.mode": "name",
        "delta.minReaderVersion": "2",
        "delta.minWriterVersion": "5",
    },
)
def clean_marathos():
    # --- Ingest and union both race datasets ---
    df_main = rename_column_to_snake_case(
        spark.sql("FROM marathos.bronze.raw_marathos")
    )
    df_stockholm = rename_column_to_snake_case(
        spark.sql("FROM marathos.bronze.stockholm_trail_classic_2024")
    )
    df = df_main.unionByName(df_stockholm, allowMissingColumns=True)

    # --- Replace nulls with "Unknown" for non-critical string columns ---
    df = fill_unknown(df, ["athlete_club", "athlete_country"])

    df = (
        df
        # Validate that event unit and performance unit are consistent
        .transform(validate_performance_units)
        # Derive event_type column (distance or time)
        .transform(parse_event_type)
        # Parse event_dates string into start/end date columns
        .transform(parse_event_dates)
        # Parse athlete_performance into performance_seconds and performance_km
        .transform(parse_performance)
        # Remove unrealistic performance values per event type
        .transform(filter_realistic_performance)
        # Cast and validate athlete_average_speed, fill nulls with 0.0
        .transform(filter_athlete_speed)
        # Drop rows where critical fields are null
        .transform(
            lambda df: drop_null_rows(
                df,
                [
                    "athlete_performance",
                    "athlete_id",
                    "athlete_gender",
                    "athlete_age_category",
                    "event_distance/length",
                    "performance_seconds",
                    "event_start_date",
                    "athlete_year_of_birth",
                ],
            )
        )
        # Remove athletes outside valid age range (18–100) at time of event
        .transform(filter_athlete_age)
        # Normalize age category prefix (F -> W)
        .transform(normalize_age_category)
        # Generate surrogate keys for event and result
        .transform(generate_ids)
        # Remove leading asterisks from club names
        .transform(clean_club_names)
    )

    # --- Join country codes (inner join drops rows with unknown countries) ---
    country_codes = spark.sql("FROM marathos.bronze.country_codes")
    df = df.join(country_codes, on="athlete_country", how="inner")

    return df