import org.apache.spark.sql.{SaveMode, SparkSession}

object DataFrameFromFileCSV {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExample")
      .getOrCreate();

    val df2 = spark.read.option("header", true).csv("/apps/FirstSparkProject/src/resources/Ages.csv")
    //val df2 = spark.read.options(Map("delimiter"->",")).csv("src/main/resources/zipcodes.csv")
    //val df2 = spark.read.options(Map("inferSchema" -> "true", "delimiter" -> ",", "header" -> "true"))
      //.csv("src/main/resources/zipcodes.csv")
    //val df = spark.read.csv("Folder path")
    //val df2 = spark.read
      //.text("/src/resources/file.txt")
    df2.show()

    /*
    val schema = new StructType()
          .add("RecordNumber",IntegerType,true)
          .add("Zipcode",IntegerType,true)
          .add("ZipCodeType",StringType,true)
          .add("City",StringType,true)
          .add("State",StringType,true)
          .add("LocationType",StringType,true)
          .add("Lat",DoubleType,true)
          .add("Long",DoubleType,true)
          .add("Xaxis",IntegerType,true)
          .add("Yaxis",DoubleType,true)
          .add("Zaxis",DoubleType,true)
          .add("WorldRegion",StringType,true)
          .add("Country",StringType,true)
          .add("LocationText",StringType,true)
          .add("Location",StringType,true)
          .add("Decommisioned",BooleanType,true)
          .add("TaxReturnsFiled",StringType,true)
          .add("EstimatedPopulation",IntegerType,true)
          .add("TotalWages",IntegerType,true)
          .add("Notes",StringType,true)
    val df_with_schema = spark.read.format("csv")
          .option("header", "true")
          .schema(schema)
          .load("src/main/resources/zipcodes.csv")
    df_with_schema.printSchema()
    df_with_schema.show(false)

     */

    df2.write.option("header", true).mode(SaveMode.Overwrite)
      .csv("/apps/Ages2")
  }
}
