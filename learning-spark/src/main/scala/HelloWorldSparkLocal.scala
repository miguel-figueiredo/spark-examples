import org.apache.spark.sql.SparkSession

object HelloWorldSparkLocal {
  def main(args: Array[String]): Unit ={

    // Start the cluster with docker-compose before executing
    val spark = SparkSession.builder()
      .master("local")
      .appName("SparkByExample")
      .getOrCreate()

    import spark.implicits._

    println("Hello, world!")
    val rdd = spark.sparkContext.parallelize(1 to 1000);
    val dataset = spark.createDataset(rdd)
    println("Sum: " + rdd.sum())

    spark.stop()
  }
}

