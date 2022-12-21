SBT File ==================================================================================================

ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.9"

lazy val root = (project in file("."))
  .settings(
    name := "FirstSparkProject"
  )

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.3.0"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.3.0"

// https://mvnrepository.com/artifact/com.databricks/spark-xml
libraryDependencies += "com.databricks" %% "spark-xml" % "0.15.0"

// https://mvnrepository.com/artifact/org.apache.avro/avro
libraryDependencies += "org.apache.avro" % "avro" % "1.11.1"



SBT File ==================================================================================================