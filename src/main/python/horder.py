# exploding the nest structure into individual rows and applying some function and then re-creating the nested strucuture
# Building a user-defined function

# builtin functions for complex data types
# array_distinct()
# array_intersect()
# array_union()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr
from pyspark.sql.types import StructField, StructType, ArrayType, IntegerType


spark: SparkSession = SparkSession.builder.appName("HorderFunctions").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

flights = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inderSchema", "true")
    .load("/app/src/main/data/flights/departuredelays.csv")
)

schema = StructType([StructField("celsius", ArrayType(IntegerType()))])

t_list = [[35, 36, 32, 30, 40, 42, 38]], [[31, 32, 34, 55, 56]]

t_c = spark.createDataFrame(t_list, schema)
t_c.createOrReplaceTempView("tC")

t_c.show()

spark.sql(
    """
    SELECT celsius, transform(celsius, t -> ((t * 9) div 5) + 32) as fahrenheit
    FROM tC
    """
)

spark.sql("""
          SELECT celsius, filter(celsius, t -> t > 38) as high
          from tC
          """).show()

spark.sql(
    """
    SELECT celsius, exists(celsius, t-> t=38) as threshold
    from tC
    """
).show()

delays_path = "/app/src/main/data/flights/departuredelays.csv"
airports_path = "/app/src/main/data/flights/airport-codes-na.txt"

airpotDF = (
    spark.read.option("header", "true")
    .option("inferSchema", "true")
    .option("delimiter", "\t")
    .csv(airports_path)
)
airpotDF.createOrReplaceTempView("airports_na")

delaysDF = spark.read.csv(delays_path, header=True, inferSchema=True)

delaysDF.createOrReplaceTempView("departure_delays")

delaysDF.withColumn("delay", expr("CAST(delay as INT) as delay")).withColumn(
    "distance", expr("CAST(distance as INT) as distance")
)

# create temporary small table
foo = delaysDF.filter(
    expr(
        """origin == 'SEA' AND destination == 'SFO' AND delay > 0 AND date LIKE '1010%'"""
    )
).orderBy(col("date"))
foo.createOrReplaceTempView("foo")

spark.sql("""
          SELECT * FROM foo;
          """).show()


bar = delaysDF.union(foo)


# joins
foo.join(airpotDF, airpotDF.IATA == foo.origin).select(
    "City", "date", "delay", "distance", "destination"
).show()


# modifications
