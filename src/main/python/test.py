from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("Simple Word Count").getOrCreate()  # type: ignore

# Sample data
data = ["hello world", "hello Spark", "hello VSCode", "hello Docker"]

# Create an RDD (Resilient Distributed Dataset)
rdd = spark.sparkContext.parallelize(data)

# Split each line into words
words = rdd.flatMap(lambda line: line.split(" "))

# Map each word to (word, 1)
word_pairs = words.map(lambda word: (word, 1))

# Reduce by key (word) to count occurrences
word_counts = word_pairs.reduceByKey(lambda a, b: a + b)

# Collect and print the results
for word, count in word_counts.collect():
    print(f"{word}: {count}")

spark.stop()
