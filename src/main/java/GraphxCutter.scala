import java.nio.ByteBuffer
import java.util.Date

import GraphxDraw.{edgeMongoRDD, readConfig, sc}
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.{ReadConfig, WriteConfig}
import com.mongodb.spark.rdd.MongoRDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.bson.{BsonDocument, BsonValue, Document}
import org.graphstream.graph.implementations.{AbstractEdge, MultiGraph, MultiNode}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
  * author: Qiao Hongbo
  * time: {$time}
  **/
case class LinkInfo(weight: Int, srcName: String, desName: String)

case class WordNotation(name: String, notation: Char, linkWeight: Int)

object GraphxCutter {
  val tool: Tool.type = Tool

  val sparkConf: SparkConf = new SparkConf()
    .setAppName("GraphDraw13")
    .setMaster("local[2]")
    .set("spark.driver.host", "localhost")
        .set("spark.mongodb.input.uri", "mongodb://" + Configuration.MONGODB_HOST + "/jd.comments_sorted")
//    .set("spark.mongodb.input.uri", "mongodb://" + Configuration.MONGODB_HOST + "/jd.comments_for_test")

  // 读取评论
  val sc = new SparkContext(sparkConf)

  def getCommentPartRDD: MongoRDD[Document] = {

    val commentsOriginalMongoRDD: MongoRDD[Document] = MongoSpark.load(sc)

    val $match: Document = Document.parse("{$match: {comment_id:{$gt:1}}}")
    val $skip: Document = Document.parse("{$skip: 320}")
    val $limit: Document = Document.parse("{$limit: 50}")

    val commentsPartMongoRDD: MongoRDD[Document] =
//      commentsOriginalMongoRDD
          commentsOriginalMongoRDD.withPipeline(Seq($match, $skip, $limit))

    commentsPartMongoRDD
  }

  //  val contentRDD: RDD[String] = commentsPartMongoRDD.map(doc => doc.get("content").asInstanceOf[String])

  // 读取字图
  val readConfigCharacter = ReadConfig(
    Map(
      "uri" -> ("mongodb://" + Configuration.MONGODB_HOST + ":27017"),
      "database" -> "jd",
      "collection" -> "all_edges")
    //      "collection" -> "graphx_edges_sample")
  )
  val characterEdgeRDD: MongoRDD[Document] = MongoSpark.load(sc, readConfigCharacter)
//  println("character:" + characterEdgeRDD.collect().length)

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

  def getCommentVerticesRDD(commentsPartMongoRDD: MongoRDD[Document]): RDD[(VertexId, Word)] = {
    commentsPartMongoRDD.flatMap(comment => {
      val cid = comment.get("comment_id").asInstanceOf[Int].toLong * 10000
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

  def getCommentEdgesRDD(commentsPartMongoRDD: MongoRDD[Document]): RDD[Edge[LinkInfo]] = {

    commentsPartMongoRDD.flatMap(comment => {
      val content = comment.get("content").asInstanceOf[String]
      val cid = comment.get("comment_id").asInstanceOf[Int].toLong * 10000
      var edgeArray = new ArrayBuffer[Edge[LinkInfo]]()

      val charArray = content.split("")

      var i = 0
      for (i <- 0 until charArray.length - 1) {
        edgeArray += Edge(cid + i, cid + i + 1, LinkInfo(0, charArray(i), charArray(i + 1)))
      }

      edgeArray.toArray[Edge[LinkInfo]]
    })
  }

  def getCommentGraph: Graph[Word, Link] = {
    val commentPartRDD = getCommentPartRDD
    val verticesRDD: RDD[(VertexId, Word)] = getCommentVerticesRDD(commentPartRDD)
    val blankEdgesRDD: RDD[Edge[LinkInfo]] = getCommentEdgesRDD(commentPartRDD)
    val blankEdgesPair: RDD[((Long, Long), (Long, Long))] = blankEdgesRDD.map(edge => {
      val srcId = tool.utf8ToLong(edge.attr.srcName)
      val dstId = tool.utf8ToLong(edge.attr.desName)
      ((srcId, dstId), (edge.srcId, edge.dstId))
    })
//    println("before join:" + blankEdgesPair.collect().length)
    val corpusEdgesRDD: RDD[Edge[Link]] = getCharacterEdgesRDD
    val corpusEdgesPair: RDD[((Long, Long), Link)] = corpusEdgesRDD.map(edge => {
      ((edge.srcId, edge.dstId), edge.attr)
    })
    val joinedEdgesRDD: RDD[((VertexId, VertexId), ((Long, Long), Option[Link]))] = blankEdgesPair.leftOuterJoin(corpusEdgesPair)
//    println("after join: " + joinedEdgesRDD.collect().length)
    val filledEdgesRDD: RDD[Edge[Link]] = joinedEdgesRDD.map(rdd => {
      val ids: (VertexId, VertexId) = rdd._2._1
      val link: Option[Link] = rdd._2._2
      Edge(ids._1, ids._2, link.getOrElse(Link(0)))
    })
    //    val graph: Graph[Word, Link] = Graph(verticesRDD, filledEdgesRDD)
    val blankEdgesTestRDD = blankEdgesRDD.map(edge => {
      Edge(edge.srcId, edge.dstId, Link(edge.attr.weight))
    })
    val graph: Graph[Word, Link] = Graph(verticesRDD, filledEdgesRDD)
    //    val graph: Graph[Word, Link] = Graph(verticesRDD, blankEdgesTestRDD)
    graph
  }

  def displayGraph(): Unit = {
    val graph: Graph[Word, Link] = getCommentGraph

    // A 代表评论开头
    // U 代表未分类
    // E 该字为词结尾
    // I 该字在词中间
    // B 该字为词开头
    val notedGraph: Graph[WordNotation, Link] = graph.mapVertices(
      (vid, word: Word) => WordNotation(word.wordName, 'A', 0))

    ////////////////////////// find 0 in-degree vertex//////////////////
    val vertexTesting: VertexRDD[WordNotation] = notedGraph.aggregateMessages[WordNotation](
      edgeContext => edgeContext.sendToDst(WordNotation(edgeContext.dstAttr.name, 'U', 0)),
      (x1: WordNotation, x2: WordNotation) => {
        var c = 'U'
        val name = x1.name
        if (x1.notation == 'U' || x2.notation == 'U') {

        } else {
          c = 'A'
        }
        WordNotation(name, c, -1)
      }
    )
    val notedGraph2: Graph[WordNotation, Link] = notedGraph.joinVertices(vertexTesting)((vid: VertexId, init: WordNotation, change: WordNotation) => change)

    val notedGraphReverse = notedGraph2.reverse
    val vertexReverse: VertexRDD[WordNotation] = notedGraphReverse.aggregateMessages[WordNotation](
      edgeContext => edgeContext.sendToDst(WordNotation(edgeContext.dstAttr.name, edgeContext.dstAttr.notation, edgeContext.attr.weight)),
      (x1, x2) => {
        val weight = x1.linkWeight
        WordNotation(x1.name, x1.notation, weight)
      }
    )
    var notedGraphReverseReverse = notedGraphReverse.reverse
    notedGraphReverseReverse = notedGraphReverseReverse.joinVertices(vertexReverse)((vid: VertexId, init: WordNotation, change: WordNotation) => change)

    //    val notationGraph: Graph[WordNotation, Link] = graph.joinVertices(vertexTesting)((vid, word: Word, tt)=>null)
    //////////////////////////// cut /////////////////////////////////////

    def vprog(vertexId: VertexId, wordNotation: WordNotation, msg: Int): WordNotation = {
      //      var wordNotation: WordNotation = wordNotation
      var note = wordNotation.notation
      if (msg == -1) {
        // 初始消息，非评论的第一个字顶点忽略
        if (wordNotation.notation == 'A') {

        }
      } else {
        // 非初始消息，msg代表输入边的权重
        val preLinkWeight = msg
        // 输出边的权重保存在顶点上
        val linkWeight = wordNotation.linkWeight
        if (linkWeight == 0) {
          note = 'E'
        } else if (preLinkWeight < 0.7 * linkWeight) {
          // 词头
          note = 'B'
        } else if ((preLinkWeight - linkWeight) / linkWeight > 0.1) {
          // 词尾
          note = 'E'
        } else {
          note = 'I'
        }
      }
      WordNotation(wordNotation.name, note, wordNotation.linkWeight)
    }

    def sendMsg(triplet: EdgeTriplet[WordNotation, Link]): Iterator[(VertexId, Int)] = {
      val dstId: VertexId = triplet.dstId
      val weight = triplet.attr.weight
      var iterator: Iterator[(VertexId, Int)] = Iterator()
      if (triplet.srcAttr.notation == 'U') {
        // 不传播
        //        iterator = Iterator[(VertexId, Int)]((dstId, 0))
      } else {
        iterator = Iterator[(VertexId, Int)]((dstId, weight))
      }
      iterator
    }

    val cutGraph = notedGraphReverseReverse.pregel(-1, 50, EdgeDirection.Out)(vprog, sendMsg, (x1, x2) => x1 + x2)

    System.setProperty("gs.ui.renderer", "org.graphstream.ui.j2dviewer.J2DGraphRenderer")
//    display(notedGraph, true, false)
//    display(notedGraph2, true, false)
//    display(notedGraphReverse, true, false)
//    display(notedGraphReverseReverse, false, true)
//    display(cutGraph, true, false)

    disgra.cutAndSave(cutGraph.mapVertices((id, tu) => (tu.name, tu.notation)).mapEdges(lin => lin.attr.weight))

  }

  def display(graph: Graph[WordNotation, Link], withNotation: Boolean, withWeight: Boolean) = {
    val wordGraph: MultiGraph = new MultiGraph("WordGraph")
    wordGraph.addAttribute("ui.stylesheet", "url(./css/styleSheet.css)")
    wordGraph.addAttribute("ui.quality")
    wordGraph.addAttribute("ui.antialias")
    wordGraph.addAttribute("layout.force", "100")
    wordGraph.addAttribute("layout.quality", "0")

    var nodeCount = 0

    for ((id, wordNotation: WordNotation) <- graph.vertices.collect()) {
      val node = wordGraph.addNode(id.toString).asInstanceOf[MultiNode]
      var label = wordNotation.name
      if (withNotation) {
        label = label + wordNotation.notation
      }
      if (withWeight){
        label = label + wordNotation.linkWeight
      }
      node.addAttribute("ui.label", label)
    }
    //    println("node count = " + nodeCount)

    for (Edge(src, des, link: Link) <- graph.edges.collect()) {
      val edge = wordGraph.addEdge(src.toString + des.toString, src.toString, des.toString, true)
        .asInstanceOf[AbstractEdge]
      //        edge.addAttribute("ui.style","size:"+link.weight+"px;")
      edge.addAttribute("ui.label", "" + link.weight)
      edge.addAttribute("layout.weight", "0.1")
    }
    wordGraph.display()
  }

  def getCharacterVerticesRDD: RDD[(VertexId, Word)] = {
    val vertexList = List()
    val sourceVertex = characterEdgeRDD.map(edge =>
      edge.get("sourceName").asInstanceOf[String]
    )
    val desVertex = characterEdgeRDD.map(edge =>
      edge.get("desName").asInstanceOf[String]
    )
    val allVertex = sourceVertex.distinct().union(desVertex.distinct()).distinct()
    val verticesWithId: RDD[(VertexId, Word)] = allVertex.map(char => {
      val id = tool.utf8ToLong(char)
      (id, Word(char))
    })
    verticesWithId
  }

  def getCharacterEdgesRDD: RDD[Edge[Link]] = {
    val verticesRDD: RDD[(VertexId, Word)] = getCharacterVerticesRDD
    val edgesRDD: RDD[Edge[Link]] = characterEdgeRDD.map(doc => {
      val srcName = doc.get("sourceName").asInstanceOf[String]
      val desName = doc.get("desName").asInstanceOf[String]
      val weight = doc.get("weight").asInstanceOf[Int]
      val srcId = tool.utf8ToLong(srcName)
      val desId = tool.utf8ToLong(desName)
      Edge(srcId, desId, Link(weight))
    })

    edgesRDD
  }

  def makeCharacterCorpusGraph: Graph[Word, Link] = {
    val corpusVertices = getCharacterVerticesRDD
    val corpusEdges = getCharacterEdgesRDD
    val corpusGraph: Graph[Word, Link] = Graph(corpusVertices, corpusEdges)
    corpusGraph
  }

  def main(args: Array[String]): Unit = {
    displayGraph()
    //    val corpusGraph = makeCharacterCorpusGraph
    //    val edgeCount = corpusGraph.edges.collect.length
    //    println("corpus edges count: " + edgeCount)
  }
}
