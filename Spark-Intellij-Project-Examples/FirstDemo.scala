import org.apache.spark.{SparkConf, SparkContext}

object FirstDemo {

  def main (args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[1]").setAppName("FirstDemo")

    val sc = new SparkContext(conf)

    var rdd = sc.parallelize(Seq(1,2,3,4))

    //println(rdd.reduce(_+_))
    rdd.foreach(println)

  }

}
