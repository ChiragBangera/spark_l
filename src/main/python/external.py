from pyspark.sql import SparkSession
from pyspark.sql.types import LongType
from pyspark.sql.functions import (
    col,
    pandas_udf,
    lpad,
    substring,
    to_date,
    concat_ws,
)
import pandas as pd


class UDFs:
    def cubed(self, s: int):
        return s * s * s

    def pandas_cubed(self, s: pd.Series) -> pd.Series:
        return s**3


test = UDFs()
spark = SparkSession.builder.appName("udfs").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

spark.udf.register("cubed", test.cubed, LongType())
spark.range(1, 9).createOrReplaceTempView("udf_tests")


spark.sql("SELECT id, cubed(id) as id_cubed FROM udf_tests").show()


# pandas udfs
cubed_udf = pandas_udf(test.pandas_cubed, returnType=LongType())

x = pd.Series([1, 2, 3])
print(test.pandas_cubed(x))


df = spark.range(1, 4)

df.select("id", cubed_udf(col("id"))).show()

# jdbc and SQL databases
pd_df = (
    spark.read.format("jdbc")
    .option("url", "jdbc:postgresql://postgres:5432/mydb")
    .option("dbtable", "flight_delays")
    .option("user", "chirag")
    .option("password", "chirag")
    .load()
)

pd_df.show()


fdata = spark.read.csv(
    "/app/src/main/data/flights/departuredelays.csv", header=True, inferSchema=True
)

fdata = fdata.withColumnRenamed("date", "flight_date")

fdata = (
    fdata.withColumn("date_padded", lpad(col("flight_date"), 8, "0"))
    .withColumn("day", substring("date_padded", 6, 2))
    .withColumn("month", substring("date_padded", 4, 2))
    .withColumn("year_suffix", substring("date_padded", 2, 2))
    .withColumn(
        "year", concat_ws("", col("year_suffix"), col("year_suffix")).cast("int") + 1900
    )
    .withColumn(
        "flight_date", to_date(concat_ws("-", "year", "month", "day"), "yyyy-MM-dd")
    )
    .drop("date_padded", "day", "month", "year_suffix", "year")
)

fdata.show(10, truncate=False)


fdata.write.format("jdbc").option("url", "jdbc:postgresql://postgres:5432/mydb").option(
    "dbtable", "flight_delays"
).option("user", "chirag").option("password", "chirag").mode("append").save()


fdata_v1 = (
    spark.read.format("jdbc")
    .option("url", "jdbc:postgresql://postgres:5432/mydb")
    .option("dbtable", "flight_delays")
    .option("user", "chirag")
    .option("password", "chirag")
    .load()
)


fdata_v1.show()
