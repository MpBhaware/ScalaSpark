package test
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.log4j._

object USJson {
  def main (args: Array[String])
  {
    Logger.getLogger("org").setLevel(Level.OFF)
    val spark = SparkSession.builder().appName("USJson").master("local[*]").config("spark.sql.warehouse.dir", "file:///C:/temp")
      .getOrCreate()
    val data = spark.read.json("E:/Hadoop/Datasets/US/US_category_id - Copy.json")
    val res = data.select("id", "etag", "kind", "snippet.assignable", "snippet.channelId", "snippet.title")
 
 //  res.show(10, false)
    data.printSchema()
    // fields.write.mode(SaveMode.Overwrite).csv("E:/Hadoop/module/Module-5/Spark/JSONParser/test")
    
    res.createOrReplaceTempView("tab")
    
    val ress = spark.sql("select count(*) from tab")
    
    ress.show()
    
  }
}