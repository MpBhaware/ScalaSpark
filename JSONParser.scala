package test
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.log4j._

object JSONParser {
  def main (args: Array[String])
  {
    Logger.getLogger("org").setLevel(Level.OFF)
    val spark = SparkSession.builder().appName("JSONParser").master("local[*]").config("spark.sql.warehouse.dir", "file:///C:/temp")
      .getOrCreate()
    val data = spark.read.json(args(0))
    val fields = data.select("custno", "firstname","lastname", "gender","age","profession")
    fields.show(10)
    // fields.write.mode(SaveMode.Overwrite).csv("E:/Hadoop/module/Module-5/Spark/JSONParser/test")
    
  }
}