import java.io._

import GraphxDraw.graph
import org.apache.spark.graphx.{Edge, EdgeDirection, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.graphstream.graph.implementations.{AbstractEdge, MultiGraph, MultiNode}
import sun.security.provider.certpath.Vertex
import org.apache.log4j
object disgra {
  def cutAndSave(graph: Graph[(String,Char),Int]): Unit = {
    val spliter = " "
    import org.apache.log4j.PropertyConfigurator
    PropertyConfigurator.configure("./log4j.properties")
   // val sparkConf = new SparkConf()
     // .setAppName("GraphCreator13")
     // .setMaster("local[2]")
     // .set("spark.driver.host", "localhost")
     // .set("spark.mongodb.input.uri", "mongodb://" + Configuration.MONGODB_HOST + "/jd.comments_sorted")
     // .set("spark.mongodb.output.uri", "mongodb://" + Configuration.MONGODB_HOST + "/jd.graphx_nodes")
    //val sc: SparkContext = new SparkContext(sparkConf)
   // val mocknodesRDD: RDD[(VertexId, (String,Char))] = sc.parallelize(Array((0L, ("还",'S')), (1L, ("不",'B')), (2L,("",'M')),(3L, ("错",'E')), (4L, ("整",'B'))
     // , (5L, ("体",'E')), (6L, ("还",'S')), (7L, ("比",'B')), (8L, ("较",'E')), (9L, ("划",'B')), (10L, ("算",'E'))))
    //val mockedgesRDD:RDD[Edge[Int]]=sc.parallelize(Array(Edge(0L,1L,1),Edge(1L,2L,1),Edge(2L,3L,1),
     // Edge(3L,4L,1),Edge(4L,5L,1),Edge(5L,6L,1),Edge(6L,7L,1),Edge(7L,8L,1),Edge(8L,9L,1),Edge(9L,10L,1)))
   // val mocknodes2RDD:RDD[(VertexId,(String,Char))]=sc.parallelize(Array((20L, ("买",'S')), (21L, ("完",'S')), (22L, ("",'M')),(23L, ("就",'S')), (24L, ("降",'B'))
     // , (25L, ("价",'E')), (26L, ("不",'B')), (27L, ("过",'E')), (28L, ("电",'B')), (29L, ("脑",'E')), (30L, ("挺",'B')),
     // (31L, ("好",'E')),(32L, ("的",'S')),(33L, ("就",'S')),(34L, ("这",'B')),(35L, ("样",'E')),(36L, ("吧",'S'))))
    //val mockedges2RDD:RDD[Edge[Int]]=sc.parallelize(Array(Edge(20L,21L,1),Edge(21L,22L,1),Edge(22L,23L,1),
      //Edge(23L,24L,1),Edge(24L,25L,1),Edge(25L,26L,1),Edge(26L,27L,1),Edge(27L,28L,1),Edge(28L,29L,1),Edge(29L,30L,1),Edge(30L,31L,1),
    // Edge(31L,32L,1),Edge(32L,33L,1),Edge(33L,34L,1),Edge(34L,35L,1),Edge(35L,36L,1)))
    //val mockallnodes:RDD[(VertexId,(String,Char))]=mocknodesRDD.union(mocknodes2RDD)
    //val mockalledges:RDD[Edge[Int]]=mockedgesRDD.union(mockedges2RDD).distinct()
    //val graph:Graph[(String,Char),Int]=Graph(mockallnodes,mockalledges)
    //还不错，整体还比较划算
    //买完就降价emmmmm 不过电脑挺好的 就这样吧



    //val degreegra:Graph[(Int,Int),Int]=graph.mapVertices((id,tup)=>(,0)).mapEdges(e=>1)

    //val end=graph.outDegrees.filter((id:VertexId,ourdegree:Int)=>ourdegree==0)
    val end =graph.outDegrees.filter((defree)=>defree==0)//.map(yup=>yup._1)    //outdegree==0
    val start=graph.inDegrees.filter((indegree)=>indegree==0)//.map(uu=>uu._1) //indegree==0

    val tend=graph.vertices.mapValues((id,yu)=>id).minus(graph.outDegrees.mapValues((id,yu)=>id))
    val ttend=tend.map(tu=>tu._1).collect()

    val tstart=graph.vertices.mapValues((id,yu)=>id).minus(graph.inDegrees.mapValues((id,yu)=>id))
    val ttstart=tstart.map(tu=>tu._1).collect()

    //tstart.foreach(println)




    //k.vertices.foreach(println)
   // k.vertices.diff(kk).foreach(println)
   // kk.diff(k.vertices).foreach(println)
   // graph.edges.foreach(println)
    //Graph(sc.parallelize(Array((0L,1))),sc.parallelize(Array(Edge(1L,1L,2)))).outDegrees.foreach(println)
    //start.foreach(println)

    val res=graph.mapVertices((id,yu)=>{if(ttstart.contains(id)){
      (yu._1,'X')}
      else {(yu._1,yu._2)}
    }).pregel("",20)(
      (id,ownmessage,resmes)=>
        {if(resmes==""){ (resmes+ownmessage._1,ownmessage._2)}
        else {
          ownmessage._2 match {
        case 'M'=>(resmes+ownmessage._1,'X');
        case 'E'=>(resmes+ownmessage._1+spliter,'X');
        case 'B'=>(resmes+spliter+ownmessage._1,'X');
        case 'S'=>(resmes+ownmessage._1+spliter,'X');
        case 'I'=>(resmes+ownmessage._1,'X');
        case _=>(ownmessage._1,'X')
      }}},
      triplet=>{
        if(ttstart.contains(triplet.srcId)&&triplet.srcAttr._2!='X'||triplet.srcAttr._2=='X'&&triplet.dstAttr._2!='X')
        {Iterator((triplet.dstId,triplet.srcAttr._1))}
        else{
          Iterator.empty
        }
      },
      (a,b)=>a+"::"+b)
      val resarray=res.vertices.filter(tu=>ttend.contains(tu._1)).map(tu=>tu._2._1).collect()
      val writer=new PrintWriter(new File("./data/cuttingresult.txt"))
      for(str<-resarray){
        writer.write(str+"\n")
      }
      writer.close()
    //draw(graph)
  }
  def draw(graph:Graph[(String,Char),Int]): Unit ={
    System.setProperty("gs.ui.renderer", "org.graphstream.ui.j2dviewer.J2DGraphRenderer")
    val wordGraph: MultiGraph = new MultiGraph("WordGraph")
    wordGraph.addAttribute("ui.stylesheet", "url(./css/styleSheet.css)")
    wordGraph.addAttribute("ui.quality")
    wordGraph.addAttribute("ui.antialias")
    wordGraph.addAttribute("layout.force", "100")
    wordGraph.addAttribute("layout.quality", "0")

    for ((id,(rune,label)) <- graph.vertices.collect()) {
      val node = wordGraph.addNode(id.toString).asInstanceOf[MultiNode]
      node.addAttribute("ui.label", rune)
      // println("add a node")
      //      node.addAttribute("layout.weight","1000")
    }

    for (Edge(src, des, weight) <- graph.edges.collect()) {
      val edge = wordGraph.addEdge(src.toString ++ des.toString, src.toString, des.toString, true)
        .asInstanceOf[AbstractEdge]
      //        edge.addAttribute("ui.style","size:"+link.weight+"px;")
      edge.addAttribute("ui.label", "" + weight)
      edge.addAttribute("layout.weight", "0.1")
    }
    wordGraph.display()

  }
}
class disgra {

}
