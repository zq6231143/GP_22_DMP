package com.作业

import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Graph_test {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val sc = new SparkContext(conf)

    val arr =sc.makeRDD( List("1	小红 20","3 小明 33","5	小七	20","7	小王 60","9 小李	20","11	小美	30","13 小花 20"))
   
    val linesRDD: RDD[Array[String]] = arr.map(arr => {
      val splited = arr.split("\\s+") // \\s+  切空格 tab 多个 +
      splited
    })

    val lines: RDD[(Long, (String, Int))] = linesRDD.map(arr => {
      val id = arr(0).toLong
      val name = arr(1)
      val age = arr(2).toInt
      (id, (name, age))
    })


    //构造点的集合
    val vertexRDD: RDD[(Long, (String, Int))] = sc.makeRDD(Seq(
      (1L, ("小红", 20)),
      (3L, ("小明", 33)),
      (5L, ("小七", 20)),
      (7L, ("小王", 60)),
      (9L, ("小李", 30)),
      (11L, ("小美", 20)),
      (13L, ("小花", 20))
    ))

    //构造边的集合
    val edge: RDD[Edge[Int]] = sc.makeRDD(Seq(
      Edge(1L, 3L, 0),
      Edge(3L, 9L, 0),
      Edge(5L, 7L, 0),
      Edge(7L, 5L, 0),
      Edge(9L, 3L, 0),
      Edge(11L, 9L, 0),
      Edge(13L, 3L, 0)
    ))

    //构件图
    val graph: Graph[(String, Int), Int] = Graph(vertexRDD,edge)
    //取出每个边上的最大顶点
    val vertices: VertexRDD[VertexId] = graph.connectedComponents().vertices
    vertices.join(vertexRDD).map{
      case(userId,(conId,(name,age))) =>{
        (conId,List(name,age))
      }
    }.reduceByKey(_++_).foreach(println)

  }
}
