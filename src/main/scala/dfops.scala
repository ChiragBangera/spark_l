import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.io.File
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.stat.Correlation
import org.apache.spark.ml.linalg.Matrix
import org.apache.spark.sql.Row


class FireSchema{
    val schema = StructType(
        Array(
            StructField("CallNumber", IntegerType, true),
            StructField("UnitID", StringType, true),
            StructField("IncidentNumber", IntegerType, true),
            StructField("CallType", StringType, true),
            StructField("CallDate", StringType, true),
            StructField("WatchDate", StringType, true),
            StructField("CallFinalDisposition", StringType, true),
            StructField("AvailableDtTm", StringType, true),
            StructField("Address", StringType, true),
            StructField("City", StringType, true),
            StructField("Zipcode", IntegerType, true),
            StructField("Battalion", StringType, true),
            StructField("StationArea", StringType, true),
            StructField("Box", StringType, true),
            StructField("OriginalPriority", StringType, true),
            StructField("Priority", StringType, true),
            StructField("FinalPriority", IntegerType, true),
            StructField("ALSUnit", BooleanType, true),
            StructField("CallTypeGroup", StringType, true),
            StructField("NumAlarms", IntegerType, true),
            StructField("UnitType", StringType, true),
            StructField("UnitSequenceInCallDispatch", IntegerType, true),
            StructField("FirePreventionDistrict", StringType, true),
            StructField("SupervisorDistrict", StringType, true),
            StructField("Neighborhood", StringType, true),
            StructField("Location", StringType, true),
            StructField("RowID", StringType, true),
            StructField("Delay", FloatType, true)
        )
    )
}

object FireOps{
    def main(args: Array[String]): Unit ={
        
        // if (args.length <= 0){
        //     println("usage FireOps <File fire dataset>")
        //     System.exit(1)
        // }

        val spark = SparkSession.builder.appName("FireReader").getOrCreate()


        // val fireFile = args(1)

        val schema = new FireSchema().schema

        // reading file 
        val fileLoc = "/app/src/main/data/sf-fire-calls.csv"

        val fireDF = spark.read.schema(schema).option("header", true).csv(fileLoc)

        fireDF.show(numRows = 10)
        // val parquetPath = "/app/src/main/data/scala/parquet/fire"
    
        // createLocalFolderIfNotExists(parquetPath)

        // saving parquet file
        // fireDF.write.mode("overwrite").format("parquet").save(parquetPath)

        val fewFireDF = fireDF
        .select("IncidentNumber", "AvailableDtTm", "CallType")
        .where(col("CallType") =!= "Medical Incident")

        fewFireDF.show(numRows = 5, truncate = false)

        val distinctFireCalls = fireDF
        .select("CallType")
        .where(col("CallType").isNotNull)
        .agg(countDistinct(col("CallType").alias("DistinctCallTypes")))
        distinctFireCalls.show()

        // renaming columns
        val renameCol = fireDF
        .withColumnRenamed("Delay", "ResponseDelayinMins")
        .select("ResponseDelayinMins")
        .where(col("ResponseDelayinMins") > 5)


        val fireTsDF = fireDF
        .withColumn("IncidentDate", to_timestamp(col("CallDate"), "MM/dd/yyyy"))
        .drop("CallDate")
        .withColumn("OnWatchDate", to_timestamp(col("WatchDate"), "MM/dd/yyyy"))
        .drop("WatchDate")
        .withColumn("AvailableDtTs", to_timestamp(col("AvailableDtTm"), "MM/dd/yyyy hh:mm:ss a"))
        .drop("AvailableDtTm")

        fireTsDF
        .select(year(col("IncidentDate")))
        .distinct()
        .orderBy(year(col("IncidentDate")))
        .show()

        fireTsDF
        .select(col("CallType"))
        .where(col("CallType").isNotNull)
        .groupBy(col("CallType"))
        .count()
        .orderBy(desc("count"))
        .show(10, false)

        // What were all the different types of fire calls in 2018?
        fireTsDF
        .select(col("CallType"))
        .where(year(col("IncidentDate"))==="2018")
        .distinct().show(truncate=false)

        // What months within the year 2018 saw the highest number of fire calls?
        fireTsDF
        .select(month(col("IncidentDate")).alias("Month"))
        .where(col("Month").isNotNull)
        .where(year(col("IncidentDate"))==="2018")
        .groupBy(col("Month"))
        .agg(count("*").alias("TotalCalls"))
        .orderBy(desc("TotalCalls"))
        .show(truncate=false)

        // Which neighborhoods had the worst response times to fire calls in 2018?
        fireTsDF
        .select(col("Neighborhood"), col("Delay"))
        .orderBy(desc("Delay"))
        .show()

        fireTsDF
        .select(col("Neighborhood"), col("Delay"))
        .where(year(col("IncidentDate")) === "2018")
        .groupBy(col("Neighborhood"))
        .agg(max(col("Delay")).alias("MaxDelay"))
        .orderBy(desc("MaxDelay"))
        .show(truncate=false)

        val fireCounts = fireDF
        .select(col("Neighborhood"), col("Zipcode"), col("CallType"))
        .groupBy(col("Neighborhood"), col("Zipcode"))
        .agg(count(col("CallType")).alias("FireCallCount"))

        // Encoding neighborhood and zipcode
        val neighborhoodIndexer = new StringIndexer()
        .setInputCol("Neighborhood")
        .setOutputCol("NeighborhoodIndexed")
        .fit(fireCounts)
        .setHandleInvalid("skip")

        val zipcodeIndexer = new StringIndexer()
        .setInputCol("Zipcode")
        .setOutputCol("ZipcodeIndexed")
        .fit(fireCounts)
        .setHandleInvalid("skip")

        val  indexedDF = zipcodeIndexer.transform(neighborhoodIndexer.transform(fireCounts))
        print(indexedDF.printSchema())

        // assemble features vector
        val assembler = new VectorAssembler()
        .setInputCols(Array("NeighborhoodIndexed", "ZipcodeIndexed", "FireCallCount"))
        .setOutputCol("features")

        val finalDf = assembler.transform(indexedDF)

        val Row(coeff: Matrix) = Correlation.corr(finalDf, "features").head()

        print(s"Correlation matrix:\n $coeff")


        println("-----------------------------------------------")

        


        
    }   

    def createLocalFolderIfNotExists(path: String): Unit = {
        val folder = new java.io.File(path)
        if (!folder.exists()){
            val created = folder.mkdirs()
            if (created) println(s"Folder created at : $path")
            else println(s"Failed to create folder at: $path")
        } else {
            println(s"Folder already present at: $path")
        }
    }
}