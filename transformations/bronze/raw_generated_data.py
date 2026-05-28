from pyspark import pipelines as dp

BASE_DIR = "/Volumes/marathos/default/raw"

# Infer schemas
country_codes_schema = (
    spark.read.format("csv")
    .options(header=True, inferSchema=True)
    .load(f"{BASE_DIR}/country_codes/country_codes.csv")
    .schema
)

stockholm_schema = (
    spark.read.format("csv")
    .options(header=True, inferSchema=True)
    .load(f"{BASE_DIR}/stockholm_trail_classic/stockholm_trail_classic_2024.csv")
    .schema
)

# Streaming tables
@dp.table(name="marathos.bronze.country_codes", 
          comment="LMM generated data based on marathos dataset column 'athlete country'",
          table_properties={
              "delta.columnMapping.mode": "name",
              "delta.minReaderVersion": "2",
              "delta.minWriterVersion": "5"
          })
def country_codes():
   return spark.readStream.format("csv").options(header=True, encoding="latin1").schema(country_codes_schema).load(f"{BASE_DIR}/country_codes/")

@dp.table(name="marathos.bronze.stockholm_trail_classic_2024", 
        comment="LMM generated data based on marathos dataset",
        table_properties={
            "delta.columnMapping.mode": "name",
            "delta.minReaderVersion": "2",
            "delta.minWriterVersion": "5"
        })
def stockholm_trail_classic():
   return spark.readStream.format("csv").options(header=True, encoding="latin1").schema(stockholm_schema).load(f"{BASE_DIR}/stockholm_trail_classic/")

