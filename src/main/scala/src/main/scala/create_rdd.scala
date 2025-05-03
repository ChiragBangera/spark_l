import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object CreateDF{
    def main(args: Array[String]): Unit ={
        val spark = SparkSession.builder.appName("AuthorAge").getOrCreate()

        val  dataDF = spark.createDataFrame(Seq(("Brooke", 20), ("Brooke", 25), 
        ("Denny", 31), ("Jules", 30), ("TD", 35))).toDF("name", "age")

        val ageDF = dataDF.groupBy(col("name")).agg(avg(col("age")))

        ageDF.show()
        spark.stop()
    }
}