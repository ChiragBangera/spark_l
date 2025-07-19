from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql import functions as f
from pyspark.sql import types as t
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable as deltatable

builder: SparkSession = (
    SparkSession.builder.config(
        "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .appName("LocalDeltaLake")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


def for_each_loan_batch(df: DataFrame, batch_id):
    loan_stream_delta_path = "/app/src/main/data/delta/streamloan"
    df.write.format("delta").mode("append").save(loan_stream_delta_path)
    print(df.count())


def process_loan_stream():
    read_loan_kafka_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("subscribe", "Test1")
        .option("startingOffsets", "earliest")
        .load()
    )

    loan_schema = (
        t.StructType()
        .add("loan_id", t.StringType())
        .add("funded_amnt", t.IntegerType())
        .add("paid_amnt", t.DoubleType())
        .add("addr_state", t.StringType())
    )

    loan_checkpoint_dir = "/app/src/main/data/checkpoints/loan"

    parsed_stream_data = (
        read_loan_kafka_stream.selectExpr("CAST(value as string) as json")
        .select(f.from_json(f.col("json"), schema=loan_schema).alias("data"))
        .select(f.col("data.*"))
    )

    loan_streaming_query = (
        parsed_stream_data.writeStream.format("delta")
        .option("checkpointLocation", loan_checkpoint_dir)
        .trigger(processingTime="10 seconds")
        .foreachBatch(for_each_loan_batch)
        .start()
    )
    # schema test
    # cols = ["loan_id", "funded_amnt", "paid_amnt", "addr_state", "closed"]
    # items = [
    #     ("1111111ss", 1000, 1000.0, "TX", True),
    #     ("2222222ss", 2000, 0.0, "CA", False),
    # ]
    # loan_test_update: DataFrame = spark.createDataFrame(items, cols).withColumn(
    #     "funded_amnt", f.col("funded_amnt").cast("int")
    # )

    # loan_test_update.write.format("delta").mode("append").option(
    #     "mergeSchema", "true"
    # ).save("/app/src/main/data/delta/streamloan")

    # df = spark.read.format("delta").load("/app/src/main/data/delta/streamloan")
    # spark.createDataFrame(df.tail(20)).show()


# process_loan_stream()
# spark.streams.awaitAnyTermination()

df = spark.read.format("delta").load("/app/src/main/data/delta/streamloan")

# updating existing data in the table in place
delta_table = deltatable.forPath(
    sparkSession=spark, path="/app/src/main/data/delta/streamloan"
)
delta_table.update("addr_state = 'CA'", {"addr_state": "'WA'"})

# deleting
delta_table.delete("paid_amnt >= 250")

# upsert logic
# (
#     delta_table.alias("t")
#     .merge(df.alias("s", "t.loan_id = s.loan_id"))
#     .whenMatchedUpdateAll()
#     .whenNotMatchedInsertAll()
#     .execute()
# )

df.show()
df.groupBy(f.col("addr_state")).count().orderBy(f.desc(f.col("count"))).show()
print(df.count())
