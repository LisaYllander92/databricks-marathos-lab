import re
from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col,
    lit,
    when,
    to_date,
    regexp_extract,
    concat,
    xxhash64,
    abs,
)
from utils.utils import rename_column_to_snake_case


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
    df = spark.sql("FROM marathos.bronze.raw_marathos")
    df = rename_column_to_snake_case(df)

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
        .withColumn(
            "event_start_date",
            to_date(
                concat(
                    regexp_extract(col("event_dates"), r"^(\d{2})", 1),
                    lit("."),
                    regexp_extract(col("event_dates"), r"(\d{2}\.\d{4})", 1),
                ),
                "dd.MM.yyyy",
            ),
        )
        .withColumn(
            "event_end_date",
            to_date(
                regexp_extract(col("event_dates"), r"(\d{2}\.\d{2}\.\d{4})$", 1),
                "dd.MM.yyyy",
            ),
        )
        .withColumn(
            "athlete_average_speed",
            when(
                col("athlete_average_speed").rlike(r"^\d+\.?\d*$"),
                col("athlete_average_speed").cast("double"),
            ).otherwise(None),
        )
        .withColumn(
            "athlete_year_of_birth", col("athlete_year_of_birth").cast("integer")
        )
        .withColumn("event_id", abs(xxhash64(col("event_name"))))
        .withColumn("result_id", abs(xxhash64(col("event_name"), col("athlete_id"))))
    )

    # Todo: add unknown on missing values
    # Add column explination for country 
