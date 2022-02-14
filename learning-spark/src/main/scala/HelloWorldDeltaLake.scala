import org.apache.spark.sql.SparkSession

object HelloWorldDeltaLake {
  def main(args: Array[String]): Unit ={

    // Start the cluster with docker-compose before executing
    val spark = SparkSession.builder()
      .master("local")
      .appName("SparkByExample")
      .getOrCreate()

    import spark.implicits._

    val df = Seq( (1, "IL", "USA"),(2, "IL", "USA"),(3, "MO", "USA"),(4, "IL", "USA"),(5, "KA", "INDIA"),(6, "MEL", "AUS")
    ).toDF("id", "state", "country")
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save("delta/myDeltaTable")

    val df1 = spark.read.format("delta").load("delta/myDeltaTable")
//    df1.show()
    df1
      .coalesce(1)
      .write
      .option("header", "true")
      .csv("/tmp/test.csv")

    spark.stop()
  }
}

