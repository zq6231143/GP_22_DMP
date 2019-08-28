package com.gaode

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 测试类
  */
object text {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName(this.getClass.getName)
      .setMaster("local[*]")
    val sc = new SparkContext(conf)

    val list = List("116.310003,39.991957")
    val rdd = sc.makeRDD(list) //创建 rdd
    val shop: RDD[String] = rdd.map(x => {
      val arr = x.split(",")
      MapUtil.getBusinessFormMap(arr(0).toDouble, arr(1).toDouble)
    })
    shop.foreach(println)

  }
}
