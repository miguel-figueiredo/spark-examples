package chapter03

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

import java.io.File
import java.nio.file.{Files, Path, Paths}

object DataFrameWrite extends App {

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
      StructField("NumAlarms", IntegerType, true),
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

  // Write to parquet (directory)
  FileUtils.forceDelete(new File("/tmp/firecalls"))
  fireCalls.write.parquet("/tmp/firecalls")

  // Write to parquet table (goes to the directory spark-warehouse
  fireCalls.write.format("parquet").saveAsTable("firecalls")
}
