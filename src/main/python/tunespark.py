from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import time
from pyspark.storagelevel import StorageLevel


spark: SparkSession = (
    SparkSession.builder.master("local[*]")
    .appName("OptimizingAndTuningSpark")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

print(spark.conf.isModifiable("spark.sql.shuffle.partitions"))

spark.sql("SET -v").select("key", "value").show(n=5, truncate=False)

spark.conf.set("spark.sql.shuffle.partitions", 5)
print(spark.conf.get("spark.sql.shuffle.partitions"))

spark.conf.set("spark.event.enabled", "true")


# to avoid job failure and resource starvation or gradual performance degradation,
# there a handfull of spark configurations. These configuratinos affect three spark components: the
# spark driver, the executor and the shuffle service running on the exrcutor.
# spark.dynamicAllocation.enabled true
# spark.dynamicAllocation.minExecutors 2
# spark.dynamicAllocation.schedulerBacklogTimeout 1m
# spark.dynamicAllocation.maxExecutors 20
# spark.dynamicAllocation.executorIdleTimeout 2min
# the size of a partition in spark is dectated by spark.sql.files.maxPartitionBytes

conf = spark.sparkContext.getConf()

print("Executors:", spark.sparkContext._jsc.sc().getExecutorMemoryStatus().keySet())
print("Executor memory:", conf.get("spark.executor.memory"))
print("Executor cores:", conf.get("spark.executor.cores"))
print("local disks:", conf.get("spark.local.directory"))

# caching
numDF = spark.range(1000 * 1000 * 1000).repartition(16)

df = spark.range(1 * 10000000).toDF("id").withColumn("square", col("id") * col("id"))
df.cache()  # cache the data
print("cache 1 start")
t1 = time.time()
df.count()  # materialize the cache
print(time.time() - t1)
print("cache 1 complete")

print("cache 2 start")
t1 = time.time()
df.count()
print(time.time() - t1)
print("cache 2 complete")


# creating a dataframe with 10 million records
df10m = spark.range(1 * 10000000).toDF("id").withColumn("square", col("id") * col("id"))
# persisting data
df10m.persist(StorageLevel.DISK_ONLY)  # serialize the data and cache it on disk

print("persist 1 start")
t1 = time.time()
df10m.count()  # materialize the cache
print(time.time() - t1)
print("persist 1 complete")

print("persist 2 start")
t1 = time.time()
df10m.count()
print(time.time() - t1)
print("persist 2 complete")


df10m.unpersist()


df10m.createOrReplaceTempView("df_table")
spark.sql("CACHE TABLE df_table")
spark.sql("SELECT count(*) FROM df_table").show()
