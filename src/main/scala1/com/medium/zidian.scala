package main.scala1.com.medium

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object zidian {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val sc = new SparkContext(conf)

    val lines: RDD[String] = sc.textFile("G:/种子/spark项目/项目day01/Spark用户画像分析/app_dict.txt")

    val tup = lines.map(x =>{
      val aa = x.split("\t")
      aa.filter(_.length <5)
      val name = aa(1)
      val com = aa(4)
      (name,com)
    })



    tup.saveAsTextFile("G:outputPath/zd-2")
  }
}
