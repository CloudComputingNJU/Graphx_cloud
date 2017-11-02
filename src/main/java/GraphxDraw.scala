import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.ReadConfig
import org.apache.commons.math.stat.Frequency
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.graphstream.graph.implementations._

import scala.util.parsing.json.{JSON, JSONObject}
import org.graphstream.graph.{Graph => GraphStream}
import org.graphstream.ui.swingViewer.Viewer

case class Word(wordName: String,frequency:Int)

case class Link(weight: Int)

object GraphxDraw extends App {
    val sparkConf = new SparkConf()
      .setAppName("GraphDraw13")
      .setMaster("local[2]")
//      .set("spark.driver.host", "localhost")
      .set("spark.mongodb.input.uri", "mongodb://zc-slave/jd.graphx_edges")
  //      .setMaster("spark://pyq-master:7077")
    //      .set("spark.driver.host", "114.212.247.255")

    val sc = new SparkContext(sparkConf);
    val readConfig = ReadConfig(
      Map(
        "uri" -> "mongodb://zc-slave:27017",
        "database" -> "jd",
        //        "collection" -> "sorted_comments",
        "collection" -> "graphx_edges"), Some(ReadConfig(sc)))
    val edgeMongoRDD = MongoSpark.load(sc, readConfig)
//    val verticeFile = "./data/testVerticeFile.json"
//    val edgeFile = "./data/testEdgeFile.json"
    def getNodesRDD():RDD[(VertexId,Word)] = {


            val nodeList = List()
            val sourceNode = edgeMongoRDD.map(edge =>
              edge.get("sourceName").asInstanceOf[String]
              )
            val desNode = edgeMongoRDD.map(edge =>
              edge.get("desName").asInstanceOf[String]
            )
            val allNode = sourceNode.distinct().union(desNode.distinct()).distinct()
            val nodesWithID = allNode.map( node => Word(node.toString(),0)).zipWithUniqueId()
            val nodesRDD=nodesWithID.map(node =>(node._2.toLong,node._1))
    //        val nodesRDD : RDD[(VertexId,Word)]= MongoSpark.load(sc, readConfig)
            println(nodesRDD.count())
            for(node <- nodesRDD){
              println(node._1,node._2.wordName)
            }
            return nodesRDD
    }

    def getEdgesRDD(): RDD[Edge[Link]] = {
      val nodesRDD:RDD[(VertexId,Word)]=getNodesRDD()
      val nameKeyRDD :RDD[(String,VertexId)]=nodesRDD.map(item => (item._2.wordName,item._1))
      val sourceKeyEdgeRDD:RDD[(String,(String,Int))]=edgeMongoRDD.map(edge =>(edge.get("sourceName").asInstanceOf[String],
          (edge.get("desName").asInstanceOf[String],edge.get("weight").asInstanceOf[Int])) )
      val desKeyEdgRDD :RDD[(String,(VertexId,Int))]= sourceKeyEdgeRDD.leftOuterJoin(nameKeyRDD).map( item =>(item._2._1._1,(item._2._2.getOrElse(0),item._2._1._2)))
      val targetEdgeRDD:RDD[((VertexId,VertexId),Int)]=desKeyEdgRDD.leftOuterJoin(nameKeyRDD).map(item => ((item._2._1._1,item._2._2.getOrElse(0)),item._2._1._2))
      val edgesRDD:RDD[Edge[Link]] = targetEdgeRDD.map(edge =>
          Edge(edge._1._1,edge._1._2,Link(edge._2))
      )
//      println(edgesRDD.count())
//      for(item <-desKeyEdgRDD){
//        println(item._2._1)
//      }
      return edgesRDD
    }


    def createGraph(): Graph[Word, Link] = {
//      val nodes = sc.textFile(verticeFile)
//      val edges = sc.textFile(edgeFile)
//      val edgesRDD: RDD[Edge[Link]] = edges.map { edge =>
//        val jsonObj = JSON.parseFull(edge)
//        val map: Map[String, Any] = jsonObj.get.asInstanceOf[Map[String, Any]]
//        Edge(map.get("sourceId").get.asInstanceOf[String].toLong,
//          map.get("desId").get.asInstanceOf[String].toLong,
//          Link(map.get("weight").get.asInstanceOf[String].toInt))
//      }
//
//      val nodesRDD: RDD[(VertexId, Word)] = nodes.map { node =>
//        val jsonObj = JSON.parseFull(node)
//        val map: Map[String, Any] = jsonObj.get.asInstanceOf[Map[String, Any]]
//        val vertexid = map.get("vertexId").get.asInstanceOf[String].toLong
//        (vertexid, Word(map.get("wordName").get.asInstanceOf[String]))
//      }
      val nodesRDD: RDD[(VertexId, Word)] = getNodesRDD();
      val edgesRDD: RDD[Edge[Link]] = getEdgesRDD();
      val graph: Graph[Word, Link] = Graph(nodesRDD, edgesRDD)
      return graph
    }

    val graph: Graph[Word, Link] = createGraph();
    graph.cache();

   System.setProperty("gs.ui.renderer", "org.graphstream.ui.j2dviewer.J2DGraphRenderer")
    val wordGraph: MultiGraph = new MultiGraph("WordGraph")
    wordGraph.addAttribute("ui.stylesheet", "url(./css/styleSheet.css)")
    wordGraph.addAttribute("ui.quality")
    wordGraph.addAttribute("ui.antialias")

    for ((id, word: Word) <- graph.vertices.collect()) {
      val node = wordGraph.addNode(id.toString).asInstanceOf[MultiNode]
      node.addAttribute("ui.label",word.wordName)
    }

    for(Edge(src,des,link:Link) <- graph.edges.collect()){
      val edge = wordGraph.addEdge(src.toString ++ des.toString,src.toString,des.toString,true)
        .asInstanceOf[AbstractEdge]
//        edge.addAttribute("ui.style","size:"+link.weight+"px;")
        edge.addAttribute("ui.label",""+link.weight)
    }
      val viewer = wordGraph.display();
  // Let the layout work ...
      viewer.disableAutoLayout();

}

