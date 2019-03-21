package com.tw

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.sql.SparkSession
// To make some of the examples work we will also need RDD
import org.apache.spark.rdd.RDD

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

/**
  * Hello world!
  *
  */
object App {
  def main(args: Array[String]): Unit = {
    //val sc = SparkSession.builder.appName("graphx").getOrCreate()
    val sc = new SparkContext(new SparkConf().setAppName("graphx").setMaster("local"))

    class VertexProperty()
    case class UserProperty(val name: String) extends VertexProperty
    case class ProductProperty(val name: String, val price: Double) extends VertexProperty







    val users: RDD[(VertexId, (String, String))] =
      sc.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
        (5L, ("franklin", "prof")), (2L, ("istoica", "prof"))))


    val relationships: RDD[Edge[String]] =
      sc.parallelize(Array(Edge(3L, 7L, "collab"), Edge(5L, 3L, "advisor"),
        Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi")))
    // Define a default user in case there are relationship with missing user
    val defaultUser = ("John Doe", "Missing")
    // Build the initial Graph
    val graph = Graph(users, relationships, defaultUser)

    graph.vertices.filter {
      case (a, (b, c)) => c == "student"
    }.foreach(println)

  }
}


