name := "testProject"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies += "org.specs2" % "specs2_2.11" % "3.7"
libraryDependencies += "com.databricks" % "spark-csv_2.11" % "1.4.0"
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "1.6.1"

mainClass in (Compile, run) := Some("CreateOutputData")

    