import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row

class SchemaClass{
    val schema = StructType(
        Array(
            StructField("Id", IntegerType, false),
            StructField("First", StringType, false),
            StructField("Last", StringType, false),
            StructField("Url", StringType, false),
            StructField("Published", StringType, false),
            StructField("Hits", IntegerType, false),
            StructField("Campaigns", ArrayType(StringType), false)
        )
    )
}


object SchemaReader{
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder.appName("SchemaReaderScala").getOrCreate()

        if (args.length <= 0){
            println("usage schema <file path to blogs.json>")
            System.exit(1)
        }

        // get the path to the json file
        val jsonFile = args(0)

        val schema = new SchemaClass().schema

        val blogsDF = spark.read.schema(schema).json(jsonFile)

        blogsDF.show(false)

        // print the schema
        println(blogsDF.printSchema)
        println(blogsDF.schema)

        blogsDF.withColumn("newCol", concat(col("First"), 
        lit("-_-"), col("Last"), lit("-_-"), col("Id"))).show()

        val blogRow = Row(6, "Reynold", "Xin", "https://tinyurl.6",
         255568, "3/2/2015", Array("twitter", "LinkedIn"))

        println(blogRow(0))
        import spark.implicits._

        val rows = Seq(("Matei Zaharia", "CA"), ("Reynold Xin", "CA"))
        val authorDF = spark.createDataFrame(rows).toDF("Author", "State")
        authorDF.show()

        val authorDF2 = rows.toDF("Authors", "State")
        authorDF2.show()


        spark.stop()
    }
}