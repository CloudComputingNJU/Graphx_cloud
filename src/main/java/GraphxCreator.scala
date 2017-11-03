import org.apache.spark.{SparkConf, SparkContext}
import com.mongodb.spark
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.{ReadConfig, WriteConfig}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.bson.Document

import scala.collection.mutable.ArrayBuffer

object GraphxCreator extends App {
  val sparkConf = new SparkConf()
    .setAppName("GraphCreator13")
    .setMaster("local[2]")
        .set("spark.driver.host", "localhost")
    //      .set("spark.mongodb.input.uri", "mongodb://127.0.0.1/jd.sorted_comments")
    .set("spark.mongodb.input.uri", "mongodb://zc-slave/jd.comments_sorted")
    .set("spark.mongodb.output.uri","mongodb://zc-slave/jd.graphx_nodes")
  val sc: SparkContext = new SparkContext(sparkConf)
  def test(): Unit = {
    val readConfig = ReadConfig(
      Map(
        "uri" -> "mongodb://zc-slave:27017",
        "database" -> "jd",
        //        "collection" -> "sorted_comments",
        "collection" -> "comments_sorted"), Some(ReadConfig(sc)))


    def getCharacterPair(content: String): Array[List[String]] = {
      var characterArray = content.split("")
      var edgeArray = new ArrayBuffer[List[String]]()

      def filterCharacter(character: String): Boolean = {
        val punctuations = Array[String]("。", "，", "！", "？", "：", "；", "～", "（", "）", " ","~","?",";",".","&",
        "\0","\'","(",")","[","]",",","\\","$","@","/","?")
        for (punctuation <- punctuations) {
          if (character.equals(punctuation)||
            (character.compareToIgnoreCase("0")>=0 && character.compareToIgnoreCase("9")<=0)) {
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

    val top = sortedEdgeWeightRdd.collect() //edgeWeightRdd.sortBy((edge, weight)=>)

    println(sortedEdgeWeightRdd.count())
//    println(top)
//    for (item <- top) {
//      println(item._1 + ":" + item._2)
//    }
    val writeConfig = WriteConfig(Map("collection" -> "all_edges"), Some(WriteConfig(sc)))

    val documents = sc.parallelize(top.map(node =>

        Document.parse(s"{sourceName:'${node._1(0)}',desName:'${node._1(1)}',weight:${node._2}}")))
    MongoSpark.save(documents,writeConfig)
  }

  //  MongoSpark

    println("start")

    test()






}

object GraphxCreatorObj {

}