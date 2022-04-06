/*

import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.{SparkConf, SparkContext}

object GraphxDemo {
  def main(args: Array[String]): Unit = {
     val conf = new SparkConf().setMaster("local").setAppName("My App")
    val sc = new SparkContext(conf)

    /*
    (EmailId,MobileNumber)  = visitorId
    (a@b.com,1234)          = V1
    (b@c.com,1234)          = V2
    (b@c.com,4567)          = V3

    (c@z.com,1122)          = V4

    Output: (V1,V2,V3)
            (V4)
     */
    //val sc= sparkSession.sparkContext
    val vertices=Array((1L, "a@b.com"),(2L, "1234"),(3L,"b@c.com"),(4L, "4567"),(5L, "c@z.com"),(6L, "1122"))
    val vRDD= sc.parallelize(vertices)
    println("vertices")
    vRDD.foreach(println)

    /*(1,a@b.com)
    (2,1234)
    (3,b@c.com)
    (4,4567)
    (5,c@z.com)
    (6,1122)*/

    val edges = Array(Edge(1L,2L,"V1"),Edge(2L,3L,"V2"),Edge(3L,4L,"V3"),Edge(5L,6L,"V4"))
    val eRDD= sc.parallelize(edges)
    println("edges")
    eRDD.foreach(println)

    /*Edge(1,2,V1)
    Edge(2,3,V2)
    Edge(3,4,V3)
    Edge(5,6,V4)*/

    //val nowhere = "nowhere"

    val graph = Graph(vRDD,eRDD)
    // graph vertices
    graph.vertices.collect.foreach(println)
    /*(4,4567)
    (1,a@b.com)
    (6,1122)
    (3,b@c.com)
    (5,c@z.com)
    (2,1234)*/

    // graph edges
    graph.edges.collect.foreach(println)
    /*Edge(1,2,V1)
    Edge(2,3,V2)
    Edge(3,4,V3)
    Edge(5,6,V4)*/

    graph.triplets.take(10).foreach(println)
    /*((1,a@b.com),(2,1234),V1)
      ((2,1234),(3,b@c.com),V2)
      ((3,b@c.com),(4,4567),V3)
      ((5,c@z.com),(6,1122),V4)*/

    graph.connectedComponents().triplets.foreach(println)
    /*((1,1),(2,1),V1)
    ((2,1),(3,1),V2)
    ((3,1),(4,1),V3)
    ((5,5),(6,5),V4)*/

    val mappedGraph=graph.connectedComponents().triplets.map(edgeTriplet=> (edgeTriplet.srcAttr,edgeTriplet.attr))
    mappedGraph.foreach(println)
    /*(1,V1)
    (1,V2)
    (1,V3)
    (5,V4)*/

    val groupBySrcAtt=mappedGraph.groupBy(_._1)
    /*(1,CompactBuffer((1,V1), (1,V2), (1,V3)))
    (5,CompactBuffer((5,V4)))*/

    val onlyVisitorId=groupBySrcAtt.map(data=> {
      val cBuffer=data._2
      //Buffer is a List of Tuple2
      cBuffer.map(_._2)
    })

    onlyVisitorId.foreach(println)
    /*List(V1, V2, V3)
    List(V4)*/
  }
}
*/
