package chapter03

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import scala.collection.JavaConverters._

object DataFrameSchemas extends App {

  val spark = SparkSession.builder
    .master("local")
    .appName("DataFrameSchemas")
    .getOrCreate()

  import spark.implicits._

  // Schema with DDL
  private val ddl = "author STRING, title String, pages INT"
  spark
    .read.format("json").schema(StructType.fromDDL(ddl))
    // The books.json is a JSON array
    .option("multiline", "true")
    .load("src/main/resources/books.json")
    .printSchema()

  // Schema with StructType
  spark.read
    .option("multiline", "true")
    .schema(StructType(Array(
      StructField("author_name", StringType, false),
      StructField("book_title", StringType, false),
      StructField("page_number", IntegerType, false),
    )))
    .json("src/main/resources/books.json")
    .printSchema()

  // Infer: without a schema the pages are a string
  spark.read
    .option("multiline", "true")
    .json("src/main/resources/books.json")
    .printSchema()

  spark.read
    // The blogs.json as multiple JSON objects. Doesn't require que multiline option.
    .json("src/main/resources/blogs.json")
    .printSchema()

  // Implicits: not possible to define the schema, only the column names
  val data = Seq(
    ("Frank Herbert", "Dune", "412"),
    ("Isaac Asimov", "Foundation", "255"),
    ("Carl Sagan", "Contact", "432")
  )
  data.toDF("author", "title", "pages")
    .printSchema()
}
