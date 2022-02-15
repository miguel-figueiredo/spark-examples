package chapter03

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object ColumnsAndExpressions extends App {

  val spark = SparkSession.builder
    .master("local")
    .appName("ColumnsAndExpressions")
    .getOrCreate()

  import spark.implicits._

  val blogs = spark.read.json("src/main/resources/blogs.json")

  // Add another column
  blogs.withColumn("Big Hitters", col("Hits") > 10000).show
  // or
  blogs.withColumn("Big Hitters", expr("Hits > 10000")).show

  // Concatenate multiple columns (and a literal column)
  blogs.withColumn("Full Name", concat(col("First"), lit(" "), col("Last")))
    .select("Full Name")
    .show

  // Selecting columns
  blogs.select(expr("Hits")).show
  blogs.select(col("Hits")).show
  blogs.select("Hits").show

  // Sorting (the expr, col or $ are required)
  blogs.sort(expr("Hits").desc).show
  blogs.sort(col("Hits").desc).show
  blogs.sort($"Hits".desc).show // requires the import spark.implicits._
}
