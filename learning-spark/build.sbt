name := "learning-spark"
version := "0.1.0"
scalaVersion := "2.12.10"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.2.1" % "compile",
  "org.apache.spark" %% "spark-sql" % "3.2.1" % "compile"
)
