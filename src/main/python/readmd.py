from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('readmd_reader').getOrCreate() # type: ignore

strings = spark.read.text("/app/src/main/python/README.md")

filtered = strings.filter(strings.value.contains('Python'))

print(filtered.count())
print(filtered)
