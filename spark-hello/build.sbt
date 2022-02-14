name := "spark-hello"
version := "0.1.0"
scalaVersion := "2.12.10"

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.1.2" % "compile"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.1.2" % "compile"
libraryDependencies += "io.delta" %% "delta-core" % "1.0.0" % "compile"
