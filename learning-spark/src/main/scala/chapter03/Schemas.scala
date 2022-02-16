package chapter03

import org.apache.spark.sql.types.{
  BooleanType,
  FloatType,
  IntegerType,
  StringType,
  StructField,
  StructType
}

object Schemas {
  // Note that if the schema as less columns than the file,
  // the columns not in the schema will not be loaded to the DataFrame.
  // On the other hand if the schema has more columns than the file and
  // the field is nullable, the value will be null.
  val fireCallsSchema = StructType(
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
}
