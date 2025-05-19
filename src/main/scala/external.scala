import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparkUDFs{
    def main(args: Array[String]): Unit ={
        val spark = SparkSession.builder.appName("UGFs").getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")

        spark.udf.register("cubed", cubed)

        spark.range(1, 9).createOrReplaceTempView("udf_test")

        val fdata = spark.read
        .option("header", true)
        .option("inferSchema", true)
        .csv("/app/src/main/data/flights/departuredelays.csv")
        
        fdata.show(numRows = 10, truncate = false)

        fdata.printSchema()

        val pg_table = spark.read.format("jdbc")
        .option("url", "jdbc:postgresql://postgres:5432/mydb")
        .option("dbtable", "flight_delays")
        .option("user", "chirag")
        .option("password", "chirag")
        .load()

        pg_table.show(numRows = 10, truncate = false)

    }

    val cubed = (s: Long) => {
        s*s*s
    }


    
}
