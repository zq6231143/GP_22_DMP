package com.medium

import com.utils.RptUtils
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 媒体分析
  */
object meiti {
  def main(args: Array[String]): Unit = {


    if(args.length != 1){
      println("输入路径有问题")
      sys.exit()
    }

    val Array(input) = args
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(conf)

    val map: Map[String, String] = sc.textFile("G:/种子/spark项目/项目day01/Spark用户画像分析/app_dict.txt").map(x => {
      val arr: Array[String] = x.split("\\s", -1)
      (arr(4), arr(1))
    }).collect().toMap

    val sqlContext = new SQLContext(sc)

    val broadcast = sc.broadcast(map)

    sqlContext.read.parquet(input).map(x=> {
      val requestmode = x.getAs[Int]("requestmode")
      val processnode = x.getAs[Int]("processnode")
      val iseffective = x.getAs[Int]("iseffective")
      val isbilling = x.getAs[Int]("isbilling")
      val isbid = x.getAs[Int]("isbid")
      val iswin = x.getAs[Int]("iswin")
      val adorderid = x.getAs[Int]("adorderid")
      val winprice = x.getAs[Double]("winprice")
      val adpayment = x.getAs[Double]("adpayment")

      val appname = x.getAs[String]("appname")

      //创建三个对应的方法处理 9 个指标
      val reqlist = RptUtils.request(requestmode, processnode)
      val clicklist = RptUtils.click(requestmode, iseffective)
      val adlist = RptUtils.Ad(iseffective, isbilling, isbid, iswin, adorderid, winprice, adpayment)
      (appname, reqlist ++ clicklist ++ adlist)
    }).reduceByKey((x,y)=>x.zip(y).map(x=>x._1+x._2)).saveAsTextFile("G:outputPath/output-20190821-6")


    sc.stop()






//



  }
}
