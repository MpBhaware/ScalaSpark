package test
import org.apache.spark.SparkContext
import org.apache.log4j._
import org.apache.spark.sql.SparkSession

object sparkDataframes {
  case class customers (cust_id:Int, order_id:Int, amount:Double)
  def myFunction(line:String) : customers =
  {
    val w = line.split(",")
    val cust:customers = customers(w(0).toInt, w(1).toInt, w(2).toDouble)
    cust
  }
  
  def main(args :Array[String])
  {
    Logger.getLogger("org").setLevel(Level.OFF)
    val spark = SparkSession.builder().appName("Test").master("local[*]").config("spark.sql.warehouse.dir", "file:///C:/temp")
      .getOrCreate()
    val data = spark.sparkContext.textFile("E:/Hadoop/module/Module-5/Spark/TotalSpentByCustomer/customer-orders.csv")
    val input = data.map(myFunction)
    import spark.implicits._
    val rec = input.toDF
    rec.createOrReplaceTempView("customers_view")
    val result = spark.sql("SELECT cust_id, order_id, amount FROM customers_view WHERE (cust_id BETWEEN 0 AND 50) AND (order_id BETWEEN 3000 AND 4000) ORDER BY amount DESC")
    result.show()
  }
}