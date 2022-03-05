package test
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.log4j._

object netflixCSV {
  def main (args: Array[String])
  {
    Logger.getLogger("org").setLevel(Level.OFF)
    val spark = SparkSession.builder().appName("netflixCSV").master("local[*]").config("spark.sql.warehouse.dir", "file:///C:/temp")
      .getOrCreate()
    val data = spark.read.option("header", "true").csv("E:/Hadoop/Datasets/netflix_titles.csv")
   // val fields = data.select("custno", "firstname","lastname", "gender","age","profession")
  //  data.show(10, false)
   // data.printSchema()
    // fields.write.mode(SaveMode.Overwrite).csv("E:/Hadoop/module/Module-5/Spark/JSONParser/test")
    
    data.createOrReplaceTempView("tab")
    
    val res = spark.sql("select * from tab where director is null AND type != 'TV Show'")
    
    res.show(10)
    
  }
}