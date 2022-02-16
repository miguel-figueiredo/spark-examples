package chapter03

import chapter03.Schemas.fireCallsSchema
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{BooleanType, FloatType, IntegerType, StringType, StructField, StructType}

object DataFrameOperations extends App {

  val spark = SparkSession.builder
    .master("local")
    .appName("DataFrameOperations")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  val fireCalls = spark.read
  // Could also use
  // .option("inferSchema", "true")
    .option("header", "true")
    .schema(fireCallsSchema)
    .csv("src/main/resources/sf-fire-calls.csv")

  println("Col equality")
  fireCalls
    .select("IncidentNumber", "AvailableDtTm", "CallType")
    .where(col("CallType") === "Structure Fire")
    .show(1)

  println("Col inequality")
  fireCalls
    .select("IncidentNumber", "AvailableDtTm", "CallType")
    .where(col("CallType") =!= "Structure Fire")
    .show(1)

  println("Number of distinct CallType")
  fireCalls
    .select("CallType")
    .where(col("CallType").isNotNull)
    // using alias
    .agg(countDistinct("CallType").alias("DistinctCallType"))
    .show

  println("Number of distinct CallType if we don't filter the nulls (simpler)")
  fireCalls
  // using as with infix style
    .agg(countDistinct("CallType") as "DistinctCallTypes")
    .show

  println("The distinct CallTypes")
  fireCalls
    .select("CallType")
    .where(col("CallType").isNotNull)
    .distinct
    .show(5, false)

  println("Rename colum")
  fireCalls
    .withColumnRenamed("Delay", "ResponseDelayedInMins")
    .select("ResponseDelayedInMins")
    .where(col("ResponseDelayedInMins") > 5)
    .show(5, false)

  println("Convert StringType to TimestampType. The format argument is required because is not the default.")
  val fireCallsTs = fireCalls
    .withColumn("IncidentDate", to_timestamp(col("CallDate"), "MM/dd/yyyy"))
    .drop("CallDate")
    .withColumn(
      "AvailableDtTs",
      to_timestamp(col("AvailableDtTm"), "MM/dd/yyyy hh:mm:ss a")
    )
    .drop("AvailableDtTm")
    .select("IncidentDate", "AvailableDtTs")

  fireCallsTs.show(1, false)

  println("Use date functions on timestamp columns")
  fireCallsTs.select(year(col("IncidentDate")) as "IncidentYear")
    .distinct()
    .orderBy(year(col("IncidentDate")))
    .show

  println("The most common types of fire call")
  fireCalls.select("CallType")
    .groupBy(col("CallType"))
    .count()
    .orderBy(desc("count"))
    .show(10, false)

  println("Statistical stuff")
  fireCalls.select(
    count("NumAlarms"),
    avg("Delay"),
    min("Delay"),
    max("Delay"))
    .show()

}
