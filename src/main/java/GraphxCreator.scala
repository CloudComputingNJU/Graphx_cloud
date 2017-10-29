import org.apache.spark.{SparkConf, SparkContext}
import com.mongodb.spark
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.ReadConfig
import org.apache.spark.sql.SparkSession

object GraphxCreator extends App {
  def test() = {
    val sparkConf = new SparkConf()
      .setAppName("GraphCreator13")
      .setMaster("local[2]")
      .set("spark.mongodb.input.uri", "mongodb://127.0.0.1/jd.sorted_comments")
    val sc: SparkContext = new SparkContext(sparkConf)
    //  val spark = SparkSession.builder()
    //    .master("local")
    //    .appName("GraphCreator13")
    //    .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/jd.sorted_comments")
    //    .config("spark.driver.host", "localhost")
    //    .getOrCreate()
    val readConfig = ReadConfig(
      Map(
        "uri" -> "mongodb://localhost:27017",
        "database" -> "jd",
        "collection" -> "sorted_comments"), Some(ReadConfig(sc)))
    val commentRdd = MongoSpark.load(sc, readConfig)

    //  val commentRdd =

    println(null)
    println(commentRdd.count)
    println(commentRdd.first.toJson)
  }

  //  MongoSpark
  override def main(args: Array[String]): Unit = {
    println("start")

    test()
  }
}

object GraphxCreatorObj {

}