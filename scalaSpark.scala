package test
import org.apache.spark.SparkContext
import org.apache.log4j._

object scalaSpark {
  def myFunction(line:String) : (Int, Double) =
  {
    val w = line.split(",")
    (w(0).toInt, w(2).toDouble)
  }
  
  def main(args :Array[String])
  {
    Logger.getLogger("org").setLevel(Level.OFF)
    val sc = new SparkContext("local[*]", "scalaSpark")
    val data = sc.textFile("E:/Hadoop/module/Module-5/Spark/TotalSpentByCustomer/customer-orders.csv")
    val input = data.map(myFunction).reduceByKey(_+_).sortBy(_._2, false)
    val inn = input.filter(_._2 > 5500)
    inn.foreach(println)
  }
}