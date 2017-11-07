import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.{ReadConfig, WriteConfig}
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.bson.Document

object GraphxSave extends App {
  val sparkConf = new SparkConf()
    .setAppName("GraphDraw13")
    .setMaster("local[2]")
      .set("spark.mongodb.input.uri", "mongodb://"+Configuration.MONGODB_HOST+"/jd.all_edges")
      .set("spark.mongodb.output.uri", "mongodb://"+Configuration.MONGODB_HOST+"/jd.graphx_edgesWithID")

  val sc = new SparkContext(sparkConf)
  val readConfig = ReadConfig(
    Map(
      "uri" -> ("mongodb://" + Configuration.MONGODB_HOST + ":27017"),
      "database" -> "jd",
      "collection" -> "all_edges"), Some(ReadConfig(sc)))
  val edgeMongoRDD = MongoSpark.load(sc, readConfig)

  def saveAllNodesRDD(): RDD[(VertexId, Word)] = {
    val nodeList = List()
    val sourceNode = edgeMongoRDD.map(edge =>
      edge.get("sourceName").asInstanceOf[String]
    )
    val desNode = edgeMongoRDD.map(edge =>
      edge.get("desName").asInstanceOf[String]
    )
    val allNode = sourceNode.distinct().union(desNode.distinct()).distinct()
    val nodesWithID = allNode.map(node => Word(node.toString())).zipWithUniqueId()
    val nodesRDD = nodesWithID.map(node => (node._2.toLong, node._1))
    //        val nodesRDD : RDD[(VertexId,Word)]= MongoSpark.load(sc, readConfig)
    println(nodesRDD.count())
    val nodesRDDs: Array[(VertexId, Word)] = nodesRDD.collect()
    for (node <- nodesRDD) {
      println(node._1, node._2.wordName)
    }
    val writeConfig = WriteConfig(Map("collection" -> "graphx_vertexsWithID"), Some(WriteConfig(sc)))

    val documents = sc.parallelize(nodesRDDs.map(node =>
      Document.parse(s"{vertexId:${node._1},wordName:'${node._2.wordName}'}")))
    MongoSpark.save(documents, writeConfig)
    return nodesRDD
  }

  def saveAllEdgesRDD(): RDD[Edge[Link]] = {
    val nodesRDD: RDD[(VertexId, Word)] = saveAllNodesRDD()
    val nameKeyRDD: RDD[(String, VertexId)] = nodesRDD.map(item => (item._2.wordName, item._1))
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


  def getNodesRDD(): RDD[(VertexId, Word)] = {
    val readConfig = ReadConfig(
      Map(
//        "uri" -> "mongodb://zc-slave:27017",
        "database" -> "jd",
        "collection" -> "graphx_vertexsWithID"), Some(ReadConfig(sc)))
    val nodeMongoRDD = MongoSpark.load(sc, readConfig)
    val nodesRDD: RDD[(VertexId, Word)] = nodeMongoRDD.map(node =>
      (node.get("vertexId").asInstanceOf[Int].toString().toLong, Word(node.get("wordName").asInstanceOf[String]))
    )
    println("nodesRDD.count()"+nodesRDD.count())
    return nodesRDD
  }

  def getEdgesRDD(): RDD[Edge[Link]] = {
    val readConfig = ReadConfig(
      Map(
//        "uri" -> "mongodb://zc-slave:27017",
        "database" -> "jd",
        "collection" -> "graphx_edgesWithID"), Some(ReadConfig(sc)))
    val edgeMongoRDD = MongoSpark.load(sc, readConfig)
    val edgesRDD: RDD[Edge[Link]] = edgeMongoRDD.map(edge =>
      Edge(edge.get("sourceId").asInstanceOf[Int].toString().toLong, edge.get("desId").asInstanceOf[Int].toString().toLong, Link(edge.get("weight").asInstanceOf[Int]))
    )
    println(edgesRDD.count())
    return edgesRDD
  }

//      saveAllNodesRDD()
//      saveAllEdgesRDD()
  //    getNodesRDD()
  //    getEdgesRDD()
  val vertexRDD = getNodesRDD()
  val edgeRDD = getEdgesRDD()
  val graph: Graph[Word, Link] = Graph(vertexRDD, edgeRDD)

}
