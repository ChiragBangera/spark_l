from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql import types as t


spark: SparkSession = SparkSession.builder.appName(
    "Streaming data sources and sinks"
).getOrCreate()


fileScheme = t.StructType(
    t.StructField("key", t.IntegralType()), t.StructField("value", t.IntegralType())
)
