package chapter03

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DatasetExercise extends App {

  val spark = SparkSession
    .builder()
    .master("local")
    .appName("DatasetOperations")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._

  val iotData =
    spark.read.json("src/main/resources/iot_devices.json").as[DeviceIoTData]

  // Using map to select the column in a type safe manner,
  // but the column rename sucks :P
  println("Failing devices with battery below 3")
  iotData
    .filter(_.battery_level < 3)
    .map(d => (d.device_name, d.battery_level))
    .withColumnRenamed("_1", "Device")
    .withColumnRenamed("_2", "Battery Level")
    .orderBy(asc("Battery Level"))
    .show(false)

    println("Countries with high levels of CO2")
    iotData
      .select("cn", "c02_level")
      .groupBy("cn")
      // Only averages the c02 level.
      // Without any select it would average every number
      .avg()
      // Using sort instead of order by
      .sort($"avg(c02_level)".desc)
      .show(false)

  println("Min max for temperature, battery level, CO2 and humidity")
  iotData
    .select(
      min("temp"),
      max("temp"),
      min("battery_level"),
      max("battery_level"),
      min("c02_level"),
      max("c02_level"),
      max("humidity"),
      max("humidity")
    )
    .show(false)

  println("Sort and group by average temperature, CO2, humidity and country")
  iotData
    .select(
      "cn",
      "temp",
      "c02_level",
      "humidity"
    )
    .groupBy("cn")
    // Averages everything
    .avg()
    .sort($"avg(temp)", $"avg(c02_level)", $"avg(humidity)")
    .show(false)
}
