import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object MnMcount {
    def main(args: Array[String]):Unit={
        val spark = SparkSession.builder.appName("MmMCount").master("local[*]").getOrCreate()

        if (args.length < 1){
            print("Usage: MnMcount <mnm_file_dataset>")
            sys.exit(1)
        }

        // get the MnM filename
        val mnmFile = args(0)

        // Read file into spark dataframe
        val mnmDF = spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(mnmFile)

        // aggregate counts of all colors and groupby() state and color orderby() in descending order
        val countMnMDF = mnmDF
        .select("State", "Color", "Count")
        .groupBy("State", "Color")
        .agg(count("Count").alias("Total"))
        .orderBy(desc("Total"))

        // show resulting aggregations for all the states and colors
        countMnMDF.show(numRows = 60)
        println(s"Total Rows = ${countMnMDF.count()}")
        println()

        // find the aggregate counts for california by filtering
        val  cal_countMnMDF = mnmDF
        .select("State", "Color", "Count")
        .where(col("State") === "CA")
        .groupBy("State", "Color")
        .agg(count("Count").alias("Total"))
        .orderBy(desc("Total"))

        cal_countMnMDF.show(numRows = 10)
        spark.stop()
    }
}