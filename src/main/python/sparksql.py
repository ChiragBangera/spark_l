from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("SparkSQLPython").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


csv_file = "/app/src/main/data/departuredelays.csv"


df = (
    spark.read.format("csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .load(csv_file)
)

df.createTempView("us_daily_flights_tbl")

spark.sql("""
          SELECT distance, origin, destination
          FROM us_daily_flights_tbl
          WHERE distance > 1000
          ORDER BY distance DESC
          """).show(10)


spark.sql("""
          SELECT date, delay, origin, destination
          FROM us_daily_flights_tbl
          WHERE delay > 120 AND origin = 'SFO' AND destination = 'ORD'
          ORDER BY delay desc
          """).show(10)


parquet_file = "/app/src/main/data/flights/summary-data/parquet/2010-summary.parquet"

dfp = spark.read.format("parquet").load(parquet_file)
dfp.show(10)
