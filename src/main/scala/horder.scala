import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

object Horder{
    def main(args: Array[String]): Unit={
        val spark = SparkSession.builder.appName("HorderFunctinoScala").getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")

        import spark.implicits._

        val schema = StructType(
                        Array(
                            StructField("celsius", ArrayType(IntegerType), false)
                        )
                        )

        val t_list1 = Row(Array(35, 36, 32, 30, 40, 42, 38))
        val t_list2 = Row(Array(31, 32, 34, 55, 56))
        val tC = Seq(t_list1, t_list2)
        
        val schemaDF = spark.createDataFrame(spark.sparkContext.parallelize(tC), schema)
        schemaDF.createOrReplaceTempView("tC")

        schemaDF.show()

        // transform function
        spark.sql("""
            SELECT celsius, transform(celsius, t -> ((t * 9) div 5) + 32) as fahrenheit
            FROM tC
        """).show()

        // filter function
        spark.sql(
            """
            SELECT celsius, filter(celsius, t -> t > 38) as high
            from tc
            """
        ).show()

        // exists()
        spark.sql("""
            SELECT celsius, exists(celsius, t-> t = 38) as threshold
            from tC
        """).show()


        // reduce
        // calculate average temperature and convert to F
        spark.sql("""
            SELECT celsius, 
            reduce(celsius, 0, (t, acc) -> t + acc,
            acc -> (acc div size(celsius) * 9 div 5) + 32
            ) as avgFahrenheit
            from tC
        """).show()        
    }
}