from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from delta import configure_spark_with_delta_pip


builder: SparkSession = (
    SparkSession.builder.config(
        "spark.sql.extentions", "io.delta.sql.DeltaSparkSessionExtention"
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .appName("DeltaTestApp")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


def readwrite_load_data():
    reading_loan_df = spark.read.format("parquet").load(
        "/app/src/main/data/loans/loan-risks.snappy.parquet"
    )

    reading_loan_df.show(n=10, truncate=False)

    loan_writer = reading_loan_df.write.format("delta")
    loan_writer.mode("overwrite").save("/app/src/main/data/delta/loans")

    load_written_file = spark.read.format("delta").load(
        "/app/src/main/data/delta/loans"
    )

    load_written_file.createOrReplaceTempView("loans_delta")

    loan_sparksql = spark.sql("SELECT count(*) as total_rows FROM loans_delta")
    loan_sparksql.show()

    get_first5_records_loan_sparksql = spark.sql("SELECT * from loans_delta limit 5")
    get_first5_records_loan_sparksql.show()


readwrite_load_data()
