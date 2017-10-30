import org.apache.spark.{SparkConf, SparkContext}
import com.mongodb.spark
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.ReadConfig
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

object GraphxCreator extends App {
  def test(): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("GraphCreator13")
      .setMaster("local[2]")
      .set("spark.mongodb.input.uri", "mongodb://127.0.0.1/jd.sorted_comments")
    val sc: SparkContext = new SparkContext(sparkConf)
    val readConfig = ReadConfig(
      Map(
        "uri" -> "mongodb://localhost:27017",
        "database" -> "jd",
        "collection" -> "sorted_comments"), Some(ReadConfig(sc)))
    def getCharacterPair(content: String): Array[Array[String]] = {
      var characterArray = content.split("")
      var edgeArray = new ArrayBuffer[Array[String]]()
      var i = 0
      for (i <- 0 until characterArray.length-1){
          edgeArray += Array[String](characterArray(i), characterArray(i+1))
      }
      edgeArray.toArray[Array[String]]
//      characterArray
    }

    val commentsRdd = MongoSpark.load(sc, readConfig)
    var rs = commentsRdd
      .map(doc=>doc.get("content").asInstanceOf[String])
//      .flatMap(f)

    println(null)
    println(rs.count)
    println(commentsRdd.first)
  }

  //  MongoSpark
  override def main(args: Array[String]): Unit = {
    println("start")

    test()
  }
}

object GraphxCreatorObj {

}