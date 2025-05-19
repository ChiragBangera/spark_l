import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.source.image


object SparkSQL{
    def main(args: Array[String]): Unit ={
        val spark = SparkSession.builder.appName("SparkSQLScala").getOrCreate()

        spark.sparkContext.setLogLevel("ERROR")

        val delayData = "/app/src/main/data/departuredelays.csv"

        val df = spark.read.format("csv")
        .option("inferSchema", true)
        .option("header", true)
        .load(delayData)

        df.createTempView("us_daily_flights_tbl")

        spark.sql("""
            SELECT distance, origin, destination
            FROM us_daily_flights_tbl
            WHERE distance > 1000
            ORDER BY distance DESC
        """).show()

        // setting hive metastore spark.sql.warehouse.dir

        // creating SQL Database and tables
         spark.sql("CREATE DATABASE learn_spark_db")
         spark.sql("USE learn_spark_db")

        //  creating a managed table
        val csv_file = "/app/src/main/data/departuredelays.csv"

        // define schema
        val schema = "date STRING, delay INT, distance INT, origin STRING, destination STRING"

        val flightsDF = spark.read.schema(schema).format("csv").load(csv_file)

        // flightsDF.write.saveAsTable("managed_us_delay_flights_tbl")
        // println("Database created")

        // flightsDF.write.option("path", "/app/src/main/data/temp/us_flights_delay").saveAsTable("us_flights_delay_tbl")

        // // creating views
        // val dfSfO = spark.sql("SELECT date, delay, origin, destination FROM us_flights_delay_tbl WHERE origin = 'SFO'")
        // val dfJFK = spark.sql("SELECT date, delay, origin, destination FROM us_flights_delay_tbl WHERE origin = 'JFK'")

        // // creating temporary or globla view
        // dfSfO.createOrReplaceGlobalTempView("us_origin_airport_SFO_global_tmp_view")
        // dfJFK.createOrReplaceGlobalTempView("us_origin_airport_JFK_global_tmp_view")

        // println(spark.catalog.listTables())
        // println(spark.catalog.listDatabases())
        // println(spark.catalog.listColumns("us_flights_delay_tbl"))


        // val usFlightsDF2 = spark.table("us_flights_delay_tbl")

        // usFlightsDF2.show(truncate = false)


        val csvSchema = "DEST_COUNTRY_NAME STRING, ORIGIN_COUNTRY_NAME STRING, count INT"
        val df2 = spark.read
        .schema(csvSchema)
        .option("header", "true")
        .option("mode", "FAILFAST")
        .option("mullValue", "")
        .csv("/app/src/main/data/flights/summary-data/csv/*")

        df2.show(10, truncate = false)

        // // avro
        // val dfAvro = spark.read.format("avro")
        // .load("/app/src/main/data/flights/summary-data/avro/*")

        // dfAvro.show(10, truncate = false)

        val file = "/app/src/main/data/flights/summary-data/orc/*"
        val dfORC = spark.read.format("orc").load(file)
        dfORC.show(10, false)


        // reading images in spark
        val imageDr = "/app/src/main/data/cctvVideos/train_images/"
        val imageDF = spark.read.format("image").load(imageDr)

        imageDF.printSchema

        imageDF.select("image.height", "image.width", "image.nChannels", "image.mode", "label").show(false)

        // reading binary files
        val binaryFileDF = spark.read.format("binaryFile")
        .option("pathGlobFilter", "*.jpg")
        .option("recursiveFileLookup", "true")
        .load(imageDr)

        binaryFileDF.show(5)

    }
}