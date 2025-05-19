import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

case class DeviceIoTData (
            battery_level: Long,
            c02_level: Long,
            cca2: String,
            cca3: String,
            cn: String,
            device_id: Long,
            device_name: String,
            humidity: Long,
            ip: String,
            latitude: Double,
            lcd: String,
            longitude: Double,
            scale: String,
            temp: Long,
            timestamp: Long
        )

case class DeviceTempByCountry (
    temp: Long,
    device_name: String,
    device_id: Long,
    cca3: String
)

object Dataset{
    def main(args: Array[String]): Unit={

        val spark = SparkSession.builder.appName("DatasetAPI").getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")

        import spark.implicits._

        val ds = spark.read.json("/app/src/main/data/iot_devices.json").as[DeviceIoTData]
        ds.show(5, false)

        // data operations
        val filterTempDS = ds.filter(d => {d.temp > 30 && d.humidity > 70})
        filterTempDS.show(5, false)

        val dsTemp = ds
        .filter(d=> {d.temp > 25})
        .map(d=> (d.temp, d.device_name, d.device_id, d.cca3))
        .toDF("temp", "device_name", "device_id", "cca3")
        .as[DeviceTempByCountry]

        dsTemp.show(5, false)

        // failing devices below a certain level
        val failingDevices = ds
        .filter(d=> {d.battery_level <20})
        .map(d=> (d.device_name))
        .toDF("Failing Devices")

        failingDevices.show(10, false)

        // Identify offending countries with high c02 levels
        val averageCO2 = ds.agg(avg(col("c02_level"))).first().getDouble(0)

        val offendingCountries = ds
        .groupBy(col("cn").alias("Country"))
        .agg(avg(col("c02_level")).alias("avgc02"))
        .filter(col("avgc02") > averageCO2)
        .select(col("Country"), col("avgc02"))
        .as[(String, Double)]
        .orderBy(desc("avgc02"))

        offendingCountries.show(20, false)
    }
}