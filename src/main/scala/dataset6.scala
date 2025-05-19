import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import scala.util.Random._

case class Bloggers(
    id: BigInt,
    first: String,
    last: String,
    url:String,
    published: String,
    hits: BigInt,
    campaigns: Array[String]
)


case class Usage(
    uid: Int,
    uname: String,
    usage: Int
)

object Beans{
    def main(args: Array[String]): Unit={
        val spark = SparkSession.builder.appName("JavaBeans").getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")

        import spark.implicits._

        val blogPath = "/app/src/main/data/blogs.json"
        val blogDF = spark
        .read
        .format("json")
        .option("path", blogPath)
        .load()
        .as[Bloggers]

        blogDF.show()

        // creating sample dataset
        val r = new scala.util.Random(42)
        val data = for (i <- 0 to 1000)
            yield (Usage(i, "user-" + r.alphanumeric.take(5).mkString(""), r.nextInt(1000)))

        val dsUsage = spark.createDataset(data)
        dsUsage.show()

        // horder functions
        dsUsage
        .filter(d=> d.usage > 900)
        .orderBy(desc("usage"))
        .show(5, false)

        dsUsage.filter(filterWithUsage(_)).orderBy("usage").show(5, false)

        dsUsage.map(u => {if (u.usage > 750) u.usage * 0.15 else u.usage * 0.50}).show(5, false)

        dsUsage.map(u => {computeCostUsage(u.usage)}).show(5, false)


    }

    def filterWithUsage(u: Usage) = u.usage > 900

    def computeCostUsage(usage: Int): Double={
        if (usage > 750) usage * 0.15 else usage * 0.50
    }
}

