import org.apache.spark.sql.SparkSession

object SparkSQL {
  def main(args: Array[String]): Unit = {
    //creating a Spark Session
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExample")
      .getOrCreate();

    //RDD creation
    val schema = Seq("department", "emp_count")
    val data = Seq(("engineering", 500), ("accounts", 100), ("sales", 1500),
      ("marketing", 500), ("finance", 150))
    //RDD creation
    val rdd = spark.sparkContext.parallelize(data)
    rdd.collect()

    //Using toDF() function
    import spark.implicits._
    val toDF_df1 = rdd.toDF()
    toDF_df1.printSchema()
    toDF_df1.show()

    val toDF_df2 = rdd.toDF("department", "emp_count") //or rdd.toDF(schema:_*)
    toDF_df2.printSchema()
    toDF_df2.show()

    //using spark sql
    toDF_df2.createOrReplaceTempView("department")
    val dfsql = spark.sql("SELECT * from department")
    dfsql.printSchema()
    dfsql.show()


    //Using createDataFrame() function
    println("Using createDataFrame() function")
    val create_DF_fun = spark.createDataFrame(rdd)
    create_DF_fun.printSchema()
    create_DF_fun.show()

    //Using createDataFrame() function in conjunction with toDF()
    import spark.implicits._
    val create_DF_fun2 = spark.createDataFrame(rdd).toDF(schema: _*) //or spark.createDataFrame(rdd).toDF("department","emp_count")
    create_DF_fun2.printSchema()
    create_DF_fun2.show()

    import org.apache.spark.sql.Row
    import org.apache.spark.sql.types._
    println("using  createDataFrame() with Struct and Row Type")
    val schema2 = StructType(Array(
      StructField("department", StringType, true),
      StructField("emp_count", IntegerType, true)
    ))
    val row_rdd = rdd.map(attributes => Row(attributes._1, attributes._2))
    val df = spark.createDataFrame(row_rdd, schema2)
    df.printSchema()
    df.show()


    //create dataframe from raw data
    val data_seq = Seq(("engineering", 500), ("accounts", 100), ("sales", 1500),
      ("marketing", 500), ("finance", 150))
    val data_list = List(("engineering", 500), ("accounts", 100), ("sales", 1500),
      ("marketing", 500), ("finance", 150))

    import spark.implicits._
    println("creating DataFrame from raw Data")
    //using toDF() method
    val df3 = data_list.toDF(schema: _*)
    //using createDataFrame()
    val df4 = spark.createDataFrame(data_seq).toDF(schema: _*)
    df3.show()

    //JavaConversions is deprecated in spark and this method works only by importing it.
    /*
    import scala.collection.JavaConversions._
    import org.apache.spark.sql.types._
    import org.apache.spark.sql.Row
    println("using createDataFrame() with Row data and Structype")
    val schema2 = StructType(Array(
      StructField("department", StringType, true),
      StructField("emp_count", IntegerType, true)
    ))
    val data_seq = Seq(Row("engineering", 500), Row("accounts", 100), Row("sales", 1500),
      Row("marketing", 500), Row("finance", 150))
    val df5 = spark.createDataFrame(data_seq, schema2)
    df5.show()
    */

  }
}
