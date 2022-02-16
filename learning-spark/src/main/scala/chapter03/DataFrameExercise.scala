package chapter03

import chapter03.Schemas.fireCallsSchema
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import java.io.File

object DataFrameExercise extends App {

  val spark = SparkSession.builder
    .master("local")
    .appName("FinalExercise")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  val fireCalls = spark.read
    .option("header", "true")
    .schema(fireCallsSchema)
    .csv("src/main/resources/sf-fire-calls.csv")

  println("Different types of fire calls in 2018")
  fireCalls
    .select("CallType")
    .where(year(to_timestamp(col("CallDate"), "dd/MM/yyyy")) === 2018)
    .distinct()
    .show(false)

  println("Months in 2018 which had the highest number of calls")
  fireCalls.
    select(
      year(to_timestamp(col("CallDate"), "dd/MM/yyyy")) as "year",
      month(to_timestamp(col("CallDate"), "dd/MM/yyyy")) as "month")
    .where(col("year") === 2018)
    .groupBy("year", "month")
    .count()
    .orderBy(desc("count"))
    .show(10, false)

  println("Neighborhood that generated the most fire calls in 2018")
  fireCalls
    .select(
      "Neighborhood")
    .where(year(to_timestamp(col("CallDate"), "dd/MM/yyyy")) === 2018)
    .groupBy("Neighborhood")
    .count()
    .show(1, false)

  println("Neighborhoods with worst response time in 2018")
  fireCalls
    .select("Neighborhood", "Delay")
    .where(year(to_timestamp(col("CallDate"), "dd/MM/yyyy")) === 2018)
    .groupBy("Neighborhood")
    .avg("Delay")
    .orderBy(desc("avg(Delay)"))
    .show(false)

  println("Week in 2018 with most fire call")
  fireCalls.
    select(
      year(to_timestamp(col("CallDate"), "dd/MM/yyyy")) as "year",
      weekofyear(to_timestamp(col("CallDate"), "dd/MM/yyyy")) as "week")
    .where(col("year") === 2018)
    .groupBy("year", "week")
    .count()
    .orderBy(desc("count"))
    .show(10, false)

  println("Correlation between neighborhood, zip code and number of fire calls (??)")
  fireCalls
    .select(
      concat_ws("/", col("Neighborhood"), col("Zipcode")) as "Neighborhood/Zipcode")
    .groupBy("Neighborhood/Zipcode")
    .count()
    .select(
      corr("Neighborhood/Zipcode", "count"))
    .show(false)

  println("Write and read parquet files")
  FileUtils.forceDelete(new File("/tmp/firecalls"))
  fireCalls.write.parquet("/tmp/firecalls")

  val parquetFireCalls = spark.read.parquet("/tmp/firecalls")
  parquetFireCalls
    .select("CallType")
    .show(false)
}
