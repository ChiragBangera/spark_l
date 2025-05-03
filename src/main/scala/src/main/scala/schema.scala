import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession

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
        spark.stop()
    }
}