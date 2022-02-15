package chapter03

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object FinalExercise extends App {

  val spark = SparkSession.builder
    .master("local")
    .appName("DataFrameOperations")
    .getOrCreate()

  // Note that if the schema as less columns than the file,
  // the columns not in the schema will not be loaded to the DataFrame.
  // On the other hand if the schema has more columns than the file and
  // the field is nullable, the value will be null.
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
      StructField("OriginalPiority", StringType, true),
      StructField("Priority", StringType, true),
      StructField("FinalPriority", StringType, true),
      StructField("ASLUnit", BooleanType, true),
      StructField("CallTypeGroup", StringType, true),
      StructField("NumberAlarms", IntegerType, true),
      StructField("UnitType", StringType, true),
      StructField("UnitSequenceInCallDispatch", IntegerType, true),
      StructField("FirePreventDistrict", StringType, true),
      StructField("SupervisorDistrict", StringType, true),
      StructField("Neighborhood", StringType, true),
      StructField("Location", StringType, true),
      StructField("RowID", StringType, true),
      StructField("Delay", FloatType, true)
    )
  )

  val fireCalls = spark.read
    // Could also use
    // .option("inferSchema", "true")
    .option("header", "true")
    .schema(schema)
    .csv("src/main/resources/sf-fire-calls.csv")

  // Col equality
  fireCalls
    .select("IncidentNumber", "AvailableDtTm", "CallType")
    .where(col("CallType") === "Structure Fire")
    .show(1)

  // Col inequality
  fireCalls
    .select("IncidentNumber", "AvailableDtTm", "CallType")
    .where(col("CallType") =!= "Structure Fire")
    .show(1)

  // Number of distinct CallType
  fireCalls
    .select("CallType")
    .where(col("CallType").isNotNull)
    // using alias
    .agg(countDistinct("CallType").alias("DistinctCallType"))
    .show

  // Number of distinct CallType if we don't filter the nulls (simpler)
  fireCalls
    // using as with infix style
    .agg(countDistinct("CallType") as "DistinctCallTypes")
    .show

  // The distinct CallTypes
  fireCalls
    .select("CallType")
    .where(col("CallType").isNotNull)
    .distinct
    .show(5, false)

  // Rename colum
  fireCalls
    .withColumnRenamed("Delay", "ResponseDelayedInMins")
    .select("ResponseDelayedInMins")
    .where(col("ResponseDelayedInMins") > 5)
    .show(5, false)

  // Convert StringType to TimestampType. The format argument is required because is not the default.
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

  // Use date functions on timestamp columns
  fireCallsTs.select(year(col("IncidentDate")) as "IncidentYear")
    .distinct()
    .orderBy(year(col("IncidentDate")))
    .show

  // The most common types of fire call
  fireCalls.select("CallType")
    .groupBy(col("CallType"))
    .count()
    .orderBy(desc("count"))
    .show(10, false)

  // Statistical stuff
  fireCalls.select(
    sum("NumberAlarms"),
    avg("Delay"),
    min("Delay"),
    max("Delay"))
    .show()

}
