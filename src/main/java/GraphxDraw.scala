import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.{ReadConfig, WriteConfig}
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.bson.Document
import org.graphstream.graph.implementations._
import org.graphstream.graph.{Graph => GraphStream}

case class Word(wordName: String)

case class Link(weight: Int)

object GraphxDraw extends App {
  val sparkConf = new SparkConf()
    .setAppName("GraphDraw13")
    .setMaster("local[2]")
  //      .setMaster("spark://pyq-master:7077")
  //      .set("spark.driver.host", "localhost")
  //      .set("spark.driver.host", "114.212.247.255")
    .set("spark.mongodb.input.uri", "mongodb://" + Configuration.MONGODB_HOST + "/jd.graphx_edges")
    .set("spark.mongodb.output.uri", "mongodb://" + Configuration.MONGODB_HOST + "/jd.graphx_vertexsWithID")

  val sc = new SparkContext(sparkConf)
  val readConfig = ReadConfig(
    Map(
//      "uri" -> "mongodb://zc-slave:27017",
      "database" -> "jd",
      "collection" -> "graphx_edges"), Some(ReadConfig(sc)))
  val edgeMongoRDD = MongoSpark.load(sc, readConfig)

  def getVerticesRDD(): RDD[(VertexId, Word)] = {
    val vertexList = List()
    val sourceVertex = edgeMongoRDD.map(edge =>
      edge.get("sourceName").asInstanceOf[String]
    )
    val desVertex = edgeMongoRDD.map(edge =>
      edge.get("desName").asInstanceOf[String]
    )
    val allVertex = sourceVertex.distinct().union(desVertex.distinct()).distinct()
    val verticesWithID = allVertex.map(vertex => Word(vertex.toString)).zipWithUniqueId()
    val verticesRDD = verticesWithID.map(vertex => (vertex._2.toLong, vertex._1))
    println(verticesRDD.count())
    val verticesRDDs = verticesRDD.collect()
    for (vertex <- verticesRDD) {
      println(vertex._1, vertex._2.wordName)
    }
    return verticesRDD
  }

  def getEdgesRDD(): RDD[Edge[Link]] = {
    val verticesRDD: RDD[(VertexId, Word)] = getVerticesRDD()
    val nameKeyRDD: RDD[(String, VertexId)] = verticesRDD.map(item => (item._2.wordName, item._1))
    val sourceKeyEdgeRDD: RDD[(String, (String, Int))] = edgeMongoRDD.map(edge => (edge.get("sourceName").asInstanceOf[String],
      (edge.get("desName").asInstanceOf[String], edge.get("weight").asInstanceOf[Int])))
    val desKeyEdgRDD: RDD[(String, (VertexId, Int))] = sourceKeyEdgeRDD.leftOuterJoin(nameKeyRDD).map(item => (item._2._1._1, (item._2._2.getOrElse(0), item._2._1._2)))
    val targetEdgeRDD: RDD[((VertexId, VertexId), Int)] = desKeyEdgRDD.leftOuterJoin(nameKeyRDD).map(item => ((item._2._1._1, item._2._2.getOrElse(0)), item._2._1._2))
    val edgesRDD: RDD[Edge[Link]] = targetEdgeRDD.map(edge =>
      Edge(edge._1._1, edge._1._2, Link(edge._2))
    )

    val writeConfig = WriteConfig(Map("collection" -> "graphx_edgesWithID"), Some(WriteConfig(sc)))
    val edgesRDDs = edgesRDD.collect()
    val documents = sc.parallelize(edgesRDDs.map(edge =>
      Document.parse(s"{sourceId:${edge.srcId},desId:${edge.dstId},weight:${edge.attr.weight}}")))
    MongoSpark.save(documents, writeConfig)
    return edgesRDD
  }

  getEdgesRDD()
  getVerticesRDD()

  def createGraph(): Graph[Word, Link] = {
    val verticesRDD: RDD[(VertexId, Word)] = getVerticesRDD()
    val edgesRDD: RDD[Edge[Link]] = getEdgesRDD()
    val graph: Graph[Word, Link] = Graph(verticesRDD, edgesRDD)
    return graph
  }

  val graph: Graph[Word, Link] = createGraph()
  graph.cache()

  System.setProperty("gs.ui.renderer", "org.graphstream.ui.j2dviewer.J2DGraphRenderer")
  val wordGraph: MultiGraph = new MultiGraph("WordGraph")
  wordGraph.addAttribute("ui.stylesheet", "url(./css/styleSheet.css)")
  wordGraph.addAttribute("ui.quality")
  wordGraph.addAttribute("ui.antialias")
  wordGraph.addAttribute("layout.force", "100")
  wordGraph.addAttribute("layout.quality", "0")

  for ((id, word: Word) <- graph.vertices.collect()) {
    val node = wordGraph.addNode(id.toString).asInstanceOf[MultiNode]
    node.addAttribute("ui.label", word.wordName)
    println("add a node")
    //      node.addAttribute("layout.weight","1000")
  }

  for (Edge(src, des, link: Link) <- graph.edges.collect()) {
    val edge = wordGraph.addEdge(src.toString ++ des.toString, src.toString, des.toString, true)
      .asInstanceOf[AbstractEdge]
    //        edge.addAttribute("ui.style","size:"+link.weight+"px;")
    edge.addAttribute("ui.label", "" + link.weight)
    edge.addAttribute("layout.weight", "0.1")
  }
  wordGraph.display()
}

