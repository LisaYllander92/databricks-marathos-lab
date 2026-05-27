from pyspark import pipelines as dp
from pyspark.sql.functions import to_timestamp, col, coalesce, lit, when, round
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
        
    )