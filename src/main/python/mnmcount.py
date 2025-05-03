import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import count
import pandas as pd

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: mnmcount <file>", file=sys.stderr)
        sys.exit(-1)

    spark = SparkSession.builder.appName("PythonMnMCount").getOrCreate()  # type: ignore # type

    # get the file from command line arg
    mnm_file = sys.argv[1]

    mnm_df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(mnm_file)
    )

    count_mnm_df = (
        mnm_df.select("State", "Color", "Count")
        .groupBy("State", "Color")
        .agg(count("Count").alias("Total"))
        .orderBy("Total", ascending=False)
    )

    count_mnm_df.show(n=60, truncate=False)

    print("Total Rows = %d" % (count_mnm_df.count()))

    ca_count_mnm_df = (
        mnm_df.select("State", "Color", "Count")
        .where(mnm_df.State == "CA")
        .groupBy("State", "Color")
        .agg(count("Count").alias("Total"))
        .orderBy("Total", ascending=False)
    )

    ca_count_mnm_df.show(n=10, truncate=False)

    print(pd.DataFrame({"S": [1, 2, 3]}))

    spark.stop
