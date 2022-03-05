package test
import org.apache.spark.SparkContext
import org.apache.log4j._

object wordCount {
  def main(args :Array[String])
  {
    Logger.getLogger("org").setLevel(Level.OFF)
    val sc = new SparkContext("local[*]", "wordCount")
    val data = sc.textFile("E:/Hadoop/module/Module-5/Spark/WordCount/sample.txt")
    val w = data.flatMap(_.split(" "))
    val key = w.map(x => (x,1)).reduceByKey(_+_)
    key.foreach(println)
  }
} 