package com.tw

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object Relation {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("ConnectedComponentsCase").setMaster("local"))



    //generate graph


    val products: RDD[(VertexId, Product)] =
      sc.parallelize(Array((1L, Product(1L, "A", 1.2, "sh"))
        , (2L, Product(2L, "B", 23, "bj"))
        , (3L, Product(3L, "C", 2.9, "sh"))
        , (4L, Product(4L, "D", 45, "bj"))
      ))


    val relationships: RDD[Edge[RelationProperty]] =
      sc.parallelize(Array(Edge(1L, 2L, RelationProperty(1.2))
        , Edge(1L, 4L, RelationProperty(1.3))
        , Edge(2L, 3L, RelationProperty(1.6))
        , Edge(2L, 4L, RelationProperty(1.9))
      ))


    //Product,RelationProperty 分别定义 节点 和 边
    val graph: Graph[Product, RelationProperty] = Graph(products, relationships, Product(0L, "null", 0, "null"))
    //graph.edges.filter(_.attr.weigth > 1).foreach(println)


    val subGraph = Graph(graph.ops.collectNeighborIds(EdgeDirection.Out)
      , relationships)

    subGraph.aggregateMessages[Set[VertexId]](
      //triplet => triplet.sendToDst(triplet.srcAttr.toSet)
      triplet => triplet.sendToDst(triplet.srcAttr.toSet), (s1, s2) => s1 ++ s2
    ).mapValues[Set[VertexId]](
      (id: VertexId, neighbors: Set[VertexId]) => neighbors - id
    ).flatMap[Edge[RelationProperty]]({
      case (v: VertexId, dests: Set[VertexId]) => dests.map((dest: VertexId) => Edge(v, dest, ""))
    }
    )



    //get the subgraph
    //    graph.subgraph(_.srcId == 1L).edges.foreach( x =>{
    //      val dst = x.dstId
    //      val weight = x.attr.weigth
    //
    //      graph.subgraph(_.srcId == dst).edges.foreach(y =>{
    //        println("y:"+y)
    //        val w = (weight + y.attr.weigth)/2
    //        return (weight,y.srcId,y.dstId,w)
    //      })
    //
    //    })

  }

  def getTwoRelation(graph: Graph[Product, RelationProperty]): Unit = {


    val dstId_list = graph.edges.filter(_.srcId == 1L).map(_.dstId).collect()

    dstId_list.foreach(println)
    graph.edges
      .filter(x => {
        dstId_list.contains(x.srcId)
      }).map(_.dstId).foreach(println)

  }

  case class Product(id: Long, name: String, price: Double, location: String)

  case class RelationProperty(weigth: Double)

}
