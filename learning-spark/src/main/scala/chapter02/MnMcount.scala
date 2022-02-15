package chapter02

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * Run with:
 * spark-submit --class chapter02.MnMcount target/scala-2.12/learning-spark_2.12-0.1.0.jar src/main/resources/mnm_dataset.csv
 */
object MnMcount {

  def main(args: Array[String]) {
    val spark = SparkSession.builder
      .appName("MnMCount")
      .getOrCreate()

    if (args.length < 1) {
      print("Usage: MnMcount <mnm_file_dataset>")
      sys.exit(1)
    }
    // Get the M&M data set filename
    val mnmFile = args(0)
    // Read the file into a Spark DataFrame
    val mnmDF = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(mnmFile)
    // Aggregate counts of all colors and groupBy() State and Color
    // orderBy() in descending order
    val countMnMDF = mnmDF
      .select("State", "Color", "Count")
      .groupBy("State", "Color")
      .sum("Count")
      .orderBy(desc("sum(Count)"))
    // Show the resulting aggregations for all the states and colors
    countMnMDF.show(60)
    println(s"Total Rows = ${countMnMDF.count()}")
    println()
    // Find the aggregate counts for California by filtering
    val caCountMnMDF = mnmDF
      .select("State", "Color", "Count")
      .where(col("State") === "CA")
      .groupBy("State", "Color")
      .sum("Count")
      .orderBy(desc("sum(Count)"))
    // Show the resulting aggregations for California
    caCountMnMDF.show(10)
    // Stop the SparkSession
    spark.stop()
  }
}
