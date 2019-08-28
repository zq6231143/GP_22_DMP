package com.Tags

import com.utils.TagUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 上下文标签
  */
object sqlContext {
  def main(args: Array[String]): Unit = {

    if(args.length != 1){
      println("目录路径不对，请退出程序")
      sys.exit()
    }
    val Array(inputPath) = args
    //创建上下文
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    //读取数据
    val df: DataFrame = sqlContext.read.parquet(inputPath)
    //过滤符合条件的 userid
    val line: RDD[List[(String, Int)]] = df.filter(TagUtils.OneUserId)
      //接下来所有的标签都在内部实现
      .map(row => {
      //取出用户id  key
      val userId = TagUtils.getOneUserId(row)
      //接下来通过 row 数据 打上所有标签
      val adList = TagsAd.makeTags(row)

      adList
    })
    line.collect.toBuffer.foreach(println)






  }

}
