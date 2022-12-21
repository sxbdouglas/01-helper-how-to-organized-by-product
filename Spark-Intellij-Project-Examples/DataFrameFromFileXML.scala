//https://mvnrepository.com/artifact/org.apache.avro/avro/1.11.1

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{StringType, StructType}

object DataFrameFromFileXML {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExample")
      .getOrCreate();

    val df = spark.read
      .format("xml")
      .option("rowTag", "person")
      .load("/apps/persons.xml")

    df.printSchema()

    //val df2 = spark.read.format("com.databricks.spark.xml").option("rowTag", "person").xml("src/main/resources/persons.xml")

    df.show()

    val xmlschema = new StructType()
      .add("_id", StringType)
      .add("firstname", StringType)
      .add("middlename", StringType)
      .add("lastname", StringType)
      .add("dob_year", StringType)
      .add("dob_month", StringType)
      .add("gender", StringType)
      .add("salary", StringType)

    val df2 = spark.read
      .format("xml")
      .option("rootTag", "persons")
      .option("rowTag", "person")
      .schema(xmlschema)
      .load("/apps/persons.xml")

    df2.write
      .format("com.databricks.spark.xml")
      .mode(SaveMode.Overwrite)
      .option("rootTag", "persons")
      .option("rowTag", "person")
      .save("/apps/persons_new.xml")

  }
}
