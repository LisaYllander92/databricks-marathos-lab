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
    coalesce
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

    df = fill_unknown(df, [
    "athlete_club",
    "athlete_country",
    ])
    
    df = drop_null_rows(df, [
        "athlete_performance",
        "event_start_date",
        "athlete_id",
        "athlete_gender",
        "athlete_age_category",
        "event_distance/length",
    ])

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
    expr("try_to_date(concat(regexp_extract(`Event dates`, '^(\\\\d{2})', 1), '.', regexp_extract(`Event dates`, '(\\\\d{2}\\\\.\\\\d{4})', 1)), 'dd.MM.yyyy')")
)
.withColumn(
    "event_end_date",
    expr("try_to_date(regexp_extract(`Event dates`, '(\\\\d{2}\\\\.\\\\d{2}\\\\.\\\\d{4})$', 1), 'dd.MM.yyyy')")
)
.filter(col("event_start_date").isNotNull())
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
        .withColumn("result_id", abs(xxhash64(col("event_name"), col("athlete_id")))
        )
        .withColumn(
    "performance_seconds",
    when(
        col("Athlete performance").rlike(r"\d+:\d+:\d+"),
        regexp_extract(col("Athlete performance"), r"(\d+):(\d+):(\d+)", 1).cast("int") * 3600 +
        regexp_extract(col("Athlete performance"), r"(\d+):(\d+):(\d+)", 2).cast("int") * 60 +
        regexp_extract(col("Athlete performance"), r"(\d+):(\d+):(\d+)", 3).cast("int")
    ).otherwise(None)
)
        .withColumn(
            "Athlete club",
            coalesce(col("Athlete club").cast("string"), lit("Unknown")
                     )).withColumn(
            "Athlete country",
            coalesce(col("Athlete country").cast("string"), lit("Unknown")
                     )
        ).withColumn(
    "Athlete average speed",
    expr("try_cast(`Athlete average speed` as double)")
)
.filter(
    col("Athlete average speed").isNull() |
    (
        (col("Athlete average speed") <= 20.8) &
        (col("Athlete average speed") >= 2.0)
    )
)
.withColumn(
    "Athlete average speed",
    coalesce(col("Athlete average speed"), lit(0.0))
)
    )

    # Todo: add unknown on missing values
    # Add column explination for country 
    # släng orimliga tider / ålder
    # konvertera data typ för performance
