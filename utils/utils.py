import re
from pyspark.sql.functions import col, coalesce, lit

def to_snake_case(name):
    return re.sub(r"[\s]+", "_", name.strip().casefold())


def rename_column_to_snake_case(df):
    new_column = [to_snake_case(column) for column in df.columns]
    return df.toDF(*new_column)

def drop_null_rows(df, columns):
    for c in columns:
        df = df.filter(col(c).isNotNull())
    return df

def fill_unknown(df, columns):
    for c in columns:
        df = df.withColumn(c, coalesce(col(c).cast("string"), lit("Unknown")))
    return df