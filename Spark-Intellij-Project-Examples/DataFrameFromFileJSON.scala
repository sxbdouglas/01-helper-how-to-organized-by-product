import org.apache.spark.sql.SparkSession


object DataFrameFromFileJSON {


  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExample")
      .getOrCreate();

    val df = spark.read.json("/apps/zipcodes.json")
    df.printSchema()
    df.show(false)

    val multiline_df = spark.read.option("multiline", "true")
      .json("/apps/multiline-zipcode.json")
    multiline_df.show(false)

    //read multiple files
    /*
    val df2 = spark.read.json(
      "src/main/resources/zipcodes_streaming/zipcode1.json",
      "src/main/resources/zipcodes_streaming/zipcode2.json")
    df2.show(false)
    */

    //read all files from a folder
    //val df3 = spark.read.json("src/main/resources/zipcodes_streaming")
    //df3.show(false)


    /*
    //Define custom schema
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
    val df_with_schema = spark.read.schema(schema)
            .json("src/main/resources/zipcodes.json")
    df_with_schema.printSchema()
    df_with_schema.show(false)
     */

    /*
    spark.sqlContext.sql("CREATE TEMPORARY VIEW zipcodes USING json OPTIONS" +
      " (path '/apps/zipcodes.json')")
    spark.sqlContext.sql("select * from zipcodes").show(false)

     */

    df.write
      .json("/apps/zipcodes2")

    //df.write.mode(SaveMode.Overwrite).json("/tmp/spark_output/zipcodes.json")
    //df.write.mode(SaveMode.Append).json("/tmp/spark_output/zipcodes.json")
  }

}