from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as f
from pyspark.sql import types as t
import psycopg2


spark: SparkSession = SparkSession.builder.appName("ProcessKafkaStream1").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

kafka_schema = (
    t.StructType()
    .add("name", t.StringType())
    .add("email", t.StringType())
    .add("address", t.StringType())
    .add("time", t.TimestampType())
    .add("age", t.IntegerType())
)


# writing full batch data using jdbc into postgress
def foreach_batch_fulldata(batchdf: DataFrame, batch_id):
    (
        batchdf.write.format("jdbc")
        .option("url", "jdbc:postgresql://postgres:5432/testbd")
        .option("dbtable", "testdata")
        .option("user", "chirag")
        .option("password", "chirag")
        .mode("append")
        .save()
    )
    print("done")


# writitng state its avg age
def forech_batch_state_age(batchdf: DataFrame, batchid):
    # writing to a temp table
    (
        batchdf.select(f.col("state"), f.col("avg_age"))
        .write.format("jdbc")
        .option("url", "jdbc:postgresql://postgres:5432/testbd")
        .option("dbtable", "state_age_temp")
        .option("user", "chirag")
        .option("password", "chirag")
        .mode("overwrite")
        .save()
    )

    conn = psycopg2.connect(
        host="postgres", port=5432, database="testbd", user="chirag", password="chirag"
    )

    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("""
                    INSERT INTO state_age_avg (state, avg_age)
                    SELECT state, avg_age FROM state_age_temp
                    ON CONFLICT (state)
                    DO UPDATE SET avg_age = EXCLUDED.avg_age;
                    """)

    conn.close()


# reading from the kafka stream
read_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "random-data-test")
    .option("startingOffsets", "latest")
    .load()
)

# verifying the stream input with schema
json_df = (
    read_df.selectExpr("CAST(value as STRING) as json")
    .select(f.from_json(f.col("json"), schema=kafka_schema).alias("data"))
    .select("data.*")
)

# extracting zipcode
agg_json_df = json_df.withColumn(
    "zipcode", f.when(f.col("address").isNotNull(), f.right(f.col("address"), f.lit(5)))
)


# aggregating and extracting age and state
state_age_df = (
    json_df.select(f.col("address"), f.col("age"), f.col("time"))
    .withColumn(
        "state",
        f.when(
            f.col("address").isNotNull(),
            f.substring(f.col("address"), -8, 2),
        ),
    )
    .withColumn("time", f.to_timestamp(f.col("time")))
    .groupBy(f.col("state"))
    .agg(f.round(f.avg(f.col("age")), 2).alias("avg_age"))
)


state_age_query = (
    state_age_df.writeStream.trigger(processingTime="5 seconds")
    .foreachBatch(forech_batch_state_age)
    .outputMode("update")
    .start()
)


fulldata_query = (
    agg_json_df.writeStream.trigger(processingTime="5 seconds")
    .foreachBatch(foreach_batch_fulldata)
    .outputMode("append")
    .start()
)

spark.streams.awaitAnyTermination()
