# from pyspark.shell import spark
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import (
    StructField,
    StructType,
    IntegerType,
    StringType,
    BooleanType,
    FloatType,
)
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.stat import Correlation


fire_schema = StructType(
    [
        StructField("CallNumber", IntegerType(), True),
        StructField("UnitID", StringType(), True),
        StructField("IncidentNumber", IntegerType(), True),
        StructField("CallType", StringType(), True),
        StructField("CallDate", StringType(), True),
        StructField("WatchDate", StringType(), True),
        StructField("CallFinalDisposition", StringType(), True),
        StructField("AvailableDtTm", StringType(), True),
        StructField("Address", StringType(), True),
        StructField("City", StringType(), True),
        StructField("Zipcode", IntegerType(), True),
        StructField("Battalion", StringType(), True),
        StructField("StationArea", StringType(), True),
        StructField("Box", StringType(), True),
        StructField("OriginalPriority", StringType(), True),
        StructField("Priority", StringType(), True),
        StructField("FinalPriority", IntegerType(), True),
        StructField("ALSUnit", BooleanType(), True),
        StructField("CallTypeGroup", StringType(), True),
        StructField("NumAlarms", IntegerType(), True),
        StructField("UnitType", StringType(), True),
        StructField("UnitSequenceInCallDispatch", IntegerType(), True),
        StructField("FirePreventionDistrict", StringType(), True),
        StructField("SupervisorDistrict", StringType(), True),
        StructField("Neighborhood", StringType(), True),
        StructField("Location", StringType(), True),
        StructField("RowID", StringType(), True),
        StructField("Delay", FloatType(), True),
    ]
)

spark = SparkSession.builder.appName("FireReader").getOrCreate()

file_loc = "/app/src/main/data/sf-fire-calls.csv"

fire_df = spark.read.schema(fire_schema).option("header", True).csv(file_loc)

parquet_path = "app/src/main/data/python/parquet/fire"


fire_df.write.mode("overwrite").format("parquet").save(parquet_path)
# fire_df.write.mode("overwrite").format("parquet").saveAsTable("fireTable")

few_fire_df = fire_df.select("IncidentNumber", "AvailableDtTm", "CallType").where(
    f.col("CallType") != "Medical Incident"
)

fire_df.select("CallType").where(f.col("CallType").isNotNull()).agg(
    f.count_distinct(f.col("CallType")).alias("DistinctCallType")
).show()

few_fire_df.show(5, truncate=False)

fire_df.select("CallType").where(f.col("CallType").isNotNull()).distinct().show(
    10, truncate=False
)


# renaming column
fire_df.withColumnRenamed("Delay", "ResponseDelayedinMins").select(
    "ResponseDelayedinMins"
).where(f.col("ResponseDelayedinMins") > 5).show()

# converting datetime ts
fire_ts_df = (
    fire_df.withColumn("IncidentDate", f.to_timestamp(f.col("CallDate"), "MM/dd/yyyy"))
    .drop(f.col("CallDate"))
    .withColumn("OnWatchDate", f.to_timestamp(f.col("WatchDate"), "MM/dd/yyyy"))
    .drop(f.col("WatchDate"))
    .withColumn(
        "AvailableDtTs", f.to_timestamp(f.col("AvailableDtTm"), "MM/dd/yyyy hh:mm:ss a")
    )
    .drop(f.col("AvailableDtTm"))
)

fire_ts_df.select("IncidentDate", "OnWatchDate", "AvailableDtTs").show(5, False)


fire_counts = (
    fire_ts_df.select(f.col("Neighborhood"), f.col("Zipcode"))
    .groupBy(f.col("Neighborhood"), f.col("Zipcode"))
    .count()
    .withColumnRenamed("count", "FireCallCounts")
)

# indexing neighborhood and zipcode
neighborhood_indexer = StringIndexer(
    inputCol="Neighborhood", outputCol="NeighborhoodIndexed"
).setHandleInvalid("skip")

zipcode_indexer = StringIndexer(
    inputCol="Zipcode", outputCol="ZipcodeIndexed"
).setHandleInvalid("skip")

indexed_df = neighborhood_indexer.fit(fire_counts).transform(fire_counts)
indexed_df = zipcode_indexer.fit(indexed_df).transform(indexed_df)

# assembling
assembler = VectorAssembler(
    inputCols=["NeighborhoodIndexed", "ZipcodeIndexed", "FireCallCounts"],
    outputCol="features",
).setHandleInvalid("skip")

final_df = assembler.transform(indexed_df)

correlation_matrix = Correlation.corr(final_df, "features").head()[0]

print(correlation_matrix)
