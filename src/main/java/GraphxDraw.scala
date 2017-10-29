import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.graphstream.graph.implementations.{AbstractEdge, SingleGraph, SingleNode}

import scala.util.parsing.json.{JSON, JSONObject}

import org.graphstream.graph.{Graph => GraphStream}

case class Word(wordName: String)

case class Link(weight: Int)

object GraphxDraw extends App {
  val sparkConf = new SparkConf()
    .setAppName("Graphx13")
    .setMaster("local[2]")
  //      .setMaster("spark://pyq-master:7077")
  //      .set("spark.driver.host", "114.212.247.255")
  val sc = new SparkContext(sparkConf);
  val verticeFile = "./data/testVerticeFile.json"
  val edgeFile = "./data/testEdgeFile.json"

  def createGraph(verticeFile: String, edgeFile: String): Graph[Word, Link] = {
    val nodes = sc.textFile(verticeFile)
    val edges = sc.textFile(edgeFile)
    val edgesRDD: RDD[Edge[Link]] = edges.map { edge =>
      val jsonObj = JSON.parseFull(edge)
      val map: Map[String, Any] = jsonObj.get.asInstanceOf[Map[String, Any]]
      Edge(map.get("sourceId").get.asInstanceOf[String].toLong,
        map.get("desId").get.asInstanceOf[String].toLong,
        Link(map.get("weight").get.asInstanceOf[String].toInt))
    }

    val nodesRDD: RDD[(VertexId, Word)] = nodes.map { node =>
      val jsonObj = JSON.parseFull(node)
      val map: Map[String, Any] = jsonObj.get.asInstanceOf[Map[String, Any]]
      val vertexid = map.get("vertexId").get.asInstanceOf[String].toLong
      (vertexid, Word(map.get("wordName").get.asInstanceOf[String]))
    }
    val graph: Graph[Word, Link] = Graph(nodesRDD, edgesRDD)
    return graph
  }

  val graph: Graph[Word, Link] = createGraph(verticeFile, edgeFile);
  graph.cache();

  val graphStream: SingleGraph = new SingleGraph("GraphStream")
  graphStream.addAttribute("ui.stylesheet", "url(./css/styleSheet.css)")
  graphStream.addAttribute("ui.quality")
  graphStream.addAttribute("ui.antialias")

  for ((id, word: Word) <- graph.vertices.collect()) {
    val node = graphStream.addNode(id.toString).asInstanceOf[SingleNode]
    node.addAttribute("ui.label",word.wordName)
  }

  for(Edge(src,des,link:Link) <- graph.edges.collect()){
    val edge = graphStream.addEdge(src.toString ++ des.toString,src.toString,des.toString,true)
      .asInstanceOf[AbstractEdge]
  }
  graphStream.display()
}

