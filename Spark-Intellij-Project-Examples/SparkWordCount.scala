import org.apache.spark._
import org.apache.spark.sql.SparkSession
//scalac -classpath "spark-core_2.10-1.3.0.jar:/usr/local/spark/lib/spark-assembly-1.4.0-hadoop2.6.0.jar" SparkPi.scala
//jar -cvf wordcount.jar SparkWordCount*.class spark-core_2.10-1.3.0.jar/usr/local/spark/lib/spark-assembly-1.4.0-hadoop2.6.0.jar
//spark-submit --class SparkWordCount --master local wordcount.jar
//val broadcastVar = sc.broadcast(Array(1, 2, 3))
//val accum = sc.accumulator(0)
//
//sc.parallelize(Array(1, 2, 3, 4)).foreach(x => accum += x)
//accum.value

object SparkWordCount {
  def main(args: Array[String]) {

    //val sc = new SparkContext("local", "Word Count", "/usr/local/spark", Nil, Map(), Map())
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExample")
      .getOrCreate();

    /* local = master URL; Word Count = application name; */
    /* /usr/local/spark = Spark Home; Nil = jars; Map = environment */
    /* Map = variables to work nodes */
    /*creating an inputRDD to read text file (in.txt) through Spark context*/
    val input = spark.read
    .text("/apps/acertoreceber.txt")
    /* Transform the inputRDD into countRDD */

    /*val count = input.flatMap(line ⇒ line.split(" "))
      .map(word ⇒ (word, 1))
      .reduceByKey(_ + _)

    /* saveAsTextFile method is an action that effects on the RDD */
    count.saveAsTextFile("outfile")
    System.out.println("OK");*/
  }
}
