import org.apache.spark.{SparkConf, SparkContext}
import com.mongodb.spark
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.ReadConfig
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

object GraphxCreator extends App {
  def test(): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("GraphCreator13")
      .setMaster("local[2]")
      .set("spark.driver.host", "localhost")
      //      .set("spark.mongodb.input.uri", "mongodb://127.0.0.1/jd.sorted_comments")
      .set("spark.mongodb.input.uri", "mongodb://zc-slave/jd.comment_list_sorted")
    val sc: SparkContext = new SparkContext(sparkConf)
    val readConfig = ReadConfig(
      Map(
        "uri" -> "mongodb://zc-slave:27017",
        "database" -> "jd",
        //        "collection" -> "sorted_comments",
        "collection" -> "comment_list_sorted"), Some(ReadConfig(sc)))


    def getCharacterPair(content: String): Array[List[String]] = {
      var characterArray = content.split("")
      var edgeArray = new ArrayBuffer[List[String]]()

      def filterCharacter(character: String): Boolean = {
        val punctuations = Array[String]("。", "，", "！", "？", "：", "；", "～", "（", "）", " ")
        for (punctuation <- punctuations) {
          if (character.equals(punctuation) ||
            (character.compareToIgnoreCase("a")>=0 && character.compareToIgnoreCase("z")<=0)) {
            return false
          }
        }
        true
      }

      var i = 0
      for (i <- 0 until characterArray.length - 1) {
        val character1 = characterArray(i)
        val character2 = characterArray(i + 1)
        if (filterCharacter(character1) && filterCharacter(character2)) {
          edgeArray += List[String](character1, character2)
        }
        //          edgeArray += Array[String](characterArray(i), characterArray(i+1))
      }
      edgeArray.toArray[List[String]]
      //      characterArray
    }

    val commentsRdd = MongoSpark.load(sc, readConfig)
    val contentRdd = commentsRdd.map(doc => doc.get("content").asInstanceOf[String])
    val pairRdd = contentRdd.flatMap(getCharacterPair)
    val pairCountRdd = pairRdd.map(pair => (pair, 1))
    val edgeWeightRdd = pairCountRdd.reduceByKey((x1, x2) => x1 + x2)
    val sortedEdgeWeightRdd: RDD[(List[String], Int)] = edgeWeightRdd.map(weightedEdge => (weightedEdge._2, weightedEdge._1))
      .sortByKey(false)
      .map(reverseWeightedEdge => (reverseWeightedEdge._2, reverseWeightedEdge._1))

    val top = sortedEdgeWeightRdd.take(100) //edgeWeightRdd.sortBy((edge, weight)=>)

    println(sortedEdgeWeightRdd.count())
    println(top)
    for (item <- top) {
      println(item._1 + ":" + item._2)
    }
  }

  //  MongoSpark
  override def main(args: Array[String]): Unit = {
    println("start")

    test()
  }
}

object GraphxCreatorObj {

}