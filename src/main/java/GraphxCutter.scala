import java.util.Date

import GraphxDraw.{readConfig, sc}
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.{ReadConfig, WriteConfig}
import com.mongodb.spark.rdd.MongoRDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.bson.{BsonDocument, BsonValue, Document}
import org.graphstream.graph.implementations.{AbstractEdge, MultiGraph, MultiNode}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
  * author: Qiao Hongbo
  * time: {$time}
  **/
object GraphxCutter {
  val sparkConf: SparkConf = new SparkConf()
    .setAppName("GraphDraw13")
    .setMaster("local[2]")
    .set("spark.driver.host", "localhost")
    .set("spark.mongodb.input.uri", "mongodb://" + Configuration.MONGODB_HOST + "/jd.comments_sorted")
  //    .set("spark.mongodb.output.uri", "mongodb://" + Configuration.MONGODB_HOST + "/jd.graphx_vertexsWithID")

  // 读取评论
  val sc = new SparkContext(sparkConf)
  val commentsOriginalMongoRDD: MongoRDD[Document] = MongoSpark.load(sc)

  val $match: Document = Document.parse("{$match: {comment_id:{$gt:1}}}")
  val $skip: Document = Document.parse("{$skip: 10}")
  val $limit: Document = Document.parse("{$limit: 5}")

  val commentsPartMongoRDD: MongoRDD[Document] = commentsOriginalMongoRDD //commentsOriginalMongoRDD.withPipeline(Seq($match, $skip, $limit))

  val contentRDD: RDD[String] = commentsPartMongoRDD.map(doc => doc.get("content").asInstanceOf[String])

    println("length:"+contentRDD.collect().length)
  //  contentRDD.foreach(content => println(content))

  // 读取字图
  val readConfigCharacter = ReadConfig(
    Map(
      "uri" -> ("mongodb://"+Configuration.MONGODB_HOST+":27017"),
      "database" -> "jd",
      "collection" -> "graphx_edges"))
  val characterEdgeRDD: MongoRDD[Document] = MongoSpark.load(sc, readConfigCharacter)
  println("character:"+characterEdgeRDD.collect().length)

  def getCharacterPair(content: String): Array[List[String]] = {
    var characterArray = content.split("")
    var edgeArray = new ArrayBuffer[List[String]]()

    def filterCharacter(character: String): Boolean = {
      val punctuations = Array[String]("。", "，", "！", "？", "：", "；", "～", "（", "）", " ", "~", "?", ";", ".", "&",
        "\0", "\'", "(", ")", "[", "]", ",", "\\", "$", "@", "/", "?")
      for (punctuation <- punctuations) {
        if (character.equals(punctuation) ||
          (character.compareToIgnoreCase("0") >= 0 && character.compareToIgnoreCase("9") <= 0)) {
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

  def getVerticesRDD: RDD[(VertexId, Word)] = {
    commentsPartMongoRDD.flatMap(comment => {
      val cid = comment.get("comment_id").asInstanceOf[Int].toLong * 1000
      val content = comment.get("content").asInstanceOf[String]
      val charArray = content.split("")
      var vertices = new ArrayBuffer[(VertexId, Word)]()
      var i = 0
      for (i <- 0 until charArray.length) {
        vertices += Tuple2(cid + i, Word(charArray(i)))
      }
      vertices.toArray[(VertexId, Word)]
    })
  }

  def getEdgesRDD: RDD[Edge[Link]] = commentsPartMongoRDD.flatMap(comment => {
    val content = comment.get("content").asInstanceOf[String]
    val cid = comment.get("comment_id").asInstanceOf[Int].toLong * 1000
    var edgeArray = new ArrayBuffer[Edge[Link]]()

    val charArray = content.split("")

    var i = 0
    for (i <- 0 until charArray.length - 1) {
      edgeArray += Edge(cid + i, cid + i + 1, Link(1))
    }

    edgeArray.toArray[Edge[Link]]
  })

  def displayGraph(): Unit = {
    val verticesRDD: RDD[(VertexId, Word)] = getVerticesRDD
    val edgesRDD: RDD[Edge[Link]] = getEdgesRDD
    val graph: Graph[Word, Link] = Graph(verticesRDD, edgesRDD)
    graph.cache()

    System.setProperty("gs.ui.renderer", "org.graphstream.ui.j2dviewer.J2DGraphRenderer")
    val wordGraph: MultiGraph = new MultiGraph("WordGraph")
    wordGraph.addAttribute("ui.stylesheet", "url(./css/styleSheet.css)")
    wordGraph.addAttribute("ui.quality")
    wordGraph.addAttribute("ui.antialias")
    wordGraph.addAttribute("layout.force", "100")
    wordGraph.addAttribute("layout.quality", "0")

    var nodeCount = 0
    for ((id, word: Word) <- graph.vertices.collect()) {
      val node = wordGraph.addNode(id.toString).asInstanceOf[MultiNode]
      node.addAttribute("ui.label", word.wordName)
      nodeCount += 1
      //      node.addAttribute("layout.weight","1000")
    }
    println("node count = " + nodeCount)

    for (Edge(src, des, link: Link) <- graph.edges.collect()) {
      val edge = wordGraph.addEdge(src.toString ++ des.toString, src.toString, des.toString, true)
        .asInstanceOf[AbstractEdge]
      //        edge.addAttribute("ui.style","size:"+link.weight+"px;")
      edge.addAttribute("ui.label", "" + link.weight)
      edge.addAttribute("layout.weight", "0.1")
    }
    wordGraph.display()
  }


  def main(args: Array[String]): Unit = {
    //    displayGraph()

  }
}
