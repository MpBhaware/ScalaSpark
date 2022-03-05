package test
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.log4j._

object CSVLoader {
  def main (args: Array[String])
  {
    Logger.getLogger("org").setLevel(Level.OFF)
    val spark = SparkSession.builder().appName("CSVLoader").master("local[*]").config("spark.sql.warehouse.dir", "file:///C:/temp")
      .getOrCreate()
    val data = spark.read.option("header", "true").csv("C:/Users/Manju/Desktop/users.csv")
   // val fields = data.select("custno", "firstname","lastname", "gender","age","profession")
    data.show(10)
    // fields.write.mode(SaveMode.Overwrite).csv("E:/Hadoop/module/Module-5/Spark/JSONParser/test")
    
  }
}