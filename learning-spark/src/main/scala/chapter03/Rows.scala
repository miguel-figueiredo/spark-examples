package chapter03

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import scala.collection.JavaConverters._

object Rows extends App {

  val spark = SparkSession.builder
    .master("local")
    .appName("Rows")
    .getOrCreate()

  import spark.implicits._

  // Can be used with createDataFrame with a Java List
  val rows = List(Row("Miguel", "Figueiredo", 47)).asJava
  spark.createDataFrame(rows, StructType.fromDDL("First STRING, Last String, Age INT"))
    .show

  // The rows can be created from tuples
  Seq(("Miguel", "Figueiredo", 47)).toDF("First", "Last", "Age")
    .show
}
