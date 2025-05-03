# from pyspark.shell import sc

from pyspark.sql import SparkSession
from pyspark.sql import functions as f

# creating an RDD
# dataRDD = sc.parallelize(
#     [("Brooke", 20), ("Denny", 31), ("Jules", 30), ("TD", 35), ("Brooke", 25)]
# )

# agesRDD = (
#     dataRDD.map(lambda x: (x[0], (x[1], 1)))
#     .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
#     .map(lambda x: (x[0], x[1][0] / x[1][1]))
# )


# print(agesRDD.sample)

spark = SparkSession.builder.appName("AuthorAges").getOrCreate()  # type: ignore

# create_df
data_df = spark.createDataFrame(
    [("Brooke", 20), ("Denny", 31), ("Jules", 30), ("TD", 35), ("Brooke", 25)],
    ["name", "age"],
)

# groupby the same names together aggregate their age and compute on average

avg_age = data_df.groupBy(f.col("name")).agg(f.avg(f.col("age")))
avg_age.show()
