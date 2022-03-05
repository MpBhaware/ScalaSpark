package test
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.log4j._

object sparkSql2 {
  case class ffriends(id:Int, name:String, age:Int, friends:Int)
  def myFunction(line:String) : ffriends =
  {
    val w = line.split(",")
    val ff:ffriends = ffriends(w(0).toInt, w(1).toString(), w(2).toInt, w(3).toInt) 
    ff
  }
  
  def main(args :Array[String])
  {
    Logger.getLogger("org").setLevel(Level.OFF)
    val spark = SparkSession.builder().appName("Test").master("local[*]").config("spark.sql.warehouse.dir", "file:///C:/temp")
      .getOrCreate()
    val data = spark.sparkContext.textFile("E:/Hadoop/module/Module-5/Spark/SparkSql/fakefriends.csv")
    val input = data.map(myFunction)
    import spark.implicits._
    val rec = input.toDF
    rec.createOrReplaceTempView("friends_view")
    val result = spark.sql("SELECT name FROM friends_view WHERE age >= 13 AND age <= 19")
    result.show()
  }
  
}   