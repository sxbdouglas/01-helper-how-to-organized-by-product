import org.apache.spark.sql.SparkSession

object CreateRddDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExample")
      .getOrCreate();

    import spark.implicits._

    val columns = Seq("language", "users_count")
    val data = Seq(("Java", "20000"), ("Python", "100000"), ("Scala", "3000"))

    val rdd = spark.sparkContext.parallelize(data)

    val dfFromRDD1 = rdd.toDF()
    dfFromRDD1.printSchema()

    val dfFromRDD2 = rdd.toDF("language", "users_count")
    dfFromRDD2.printSchema()

    val dfFromRDD3 = spark.createDataFrame(rdd).toDF(columns:_*)
    dfFromRDD3.printSchema()

    //val df2 = spark.read
    //.text("/src/resources/file.txt")
  }
}
