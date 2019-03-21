package com.tw

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.GraphLoader


object ConnectedComponentsCase {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("ConnectedComponentsCase").setMaster("local"))

    val graph = GraphLoader.edgeListFile(sc, "resources/followers")

    //计算连通体
    val cc = graph.connectedComponents().vertices
    val users = sc.textFile("resources/users").map { line =>
      val field = line.split(",");
      (field(0).toLong, field(1))
    }
    val ccByUsername = users.join(cc).map {
      case (id, (username, cc)) => (username, cc)
    }


    println(ccByUsername.collect().mkString("\n"))
  }
}
