import sys
from pyspark.sql import Row, types as t
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, lit


schema = t.StructType(
    [
        t.StructField("author", t.StringType(), False),
        t.StructField("title", t.StringType(), False),
        t.StructField("pages", t.IntegralType(), False),
    ]
)

schema_DDl = "Id INT, First STRING, Last STRING, Url STRING, Published STRING, Hits INT, Campaigns ARRAY<STRING>"

data = [
    [
        1,
        "Jules",
        "Damji",
        "https://tinyurl.1",
        "1/4/2016",
        4535,
        ["twitter", "LinkedIn"],
    ],
    [
        2,
        "Brooke",
        "Wenig",
        "https://tinyurl.2",
        "5/5/2018",
        8908,
        ["twitter", "LinkedIn"],
    ],
    [
        3,
        "Denny",
        "Lee",
        "https://tinyurl.3",
        "6/7/2019",
        7659,
        ["web", "twitter", "FB", "LinkedIn"],
    ],
    [4, "Tathagata", "Das", "https://tinyurl.4", "5/12/2018", 10568, ["twitter", "FB"]],
    [
        5,
        "Matei",
        "Zaharia",
        "https://tinyurl.5",
        "5/14/2014",
        40578,
        ["web", "twitter", "FB", "LinkedIn"],
    ],
    [
        6,
        "Reynold",
        "Xin",
        "https://tinyurl.6",
        "3/2/2015",
        25568,
        ["twitter", "LinkedIn"],
    ],
]


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("usage schema < blogs.json file>")
        sys.exit(-1)

    # creating spark session
    spark = SparkSession.builder.appName("SchemaRead").getOrCreate()  # type: ignore

    # blogs_df = spark.createDataFrame(data, schema_DDl)

    # blogs_df.show()

    # print(blogs_df.printSchema())
    # print(blogs_df.schema)
    json_file = sys.argv[1]

    blogs_df = spark.read.schema(schema_DDl).json(json_file)

    blogs_df.show()

    blogs_df.select(col("Hits") * 2).show(2)

    blogs_df.withColumn("newCol", col("Hits") > 10000).show()

    # concatenating colums
    blogs_df.withColumn(
        "combinedCol", concat(col("First"), lit("_"), col("Last"), lit("_"), col("Id"))
    ).select(col("combinedCol")).show()

    # sorting by columns
    blogs_df.sort(col("Id").desc()).show()

    rows = [Row("Matei Zaharia", "CA"), Row("Reynold Xin", "CA")]
    authors_df = spark.createDataFrame(rows, ["Authors", "State"])
    authors_df.show()

    spark.stop()
