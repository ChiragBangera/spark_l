from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql import types as t
import os
import shutil


def get_spark(app_name: str = "default_app") -> SparkSession:
    spark: SparkSession = SparkSession.builder.appName(app_name).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


class Udfs:
    def __init__(self):
        pass

    @staticmethod
    def is_corrupted(value):
        return value is None or "Error" in value


class DataStreamProcessing:
    def __init__(self, spark):
        self.spark: SparkSession = spark
        udf_instance = Udfs()
        self.is_corrupted = f.udf(udf_instance.is_corrupted, t.BooleanType())

    def read_stream(self):
        lines = (
            self.spark.readStream.format("socket")
            .option("host", "localhost")
            .option("port", 9999)
            .load()
        )
        return lines

    def transform(self):
        lines = self.read_stream()
        filtered_lines = lines.filter(~self.is_corrupted(f.col("value")))
        words = filtered_lines.select(
            f.explode(f.split(f.col("value"), "\\s+")).alias("word")
        )
        counts = words.groupBy("word").count()
        return counts

    def write_data(self):
        counts = self.transform()
        writer = counts.writeStream.format("console").outputMode("complete")
        return writer

    def process_config(self):
        for s in self.spark.streams.active:  # for testing only
            s.stop()
        checkpointdir = "/app/src/main/data/checkpointdir"
        if os.path.exists(checkpointdir):
            shutil.rmtree(checkpointdir)

        writer = self.write_data()
        streaming_query = (
            writer.trigger(processingTime="1 second")
            .option("checkpointLocation", checkpointdir)
            .start()
        )
        streaming_query.awaitTermination()


sample = DataStreamProcessing(get_spark("test"))

sample.process_config()
