package com.redis_idea

import com.Appname.{TagApp, TagstopWord}
import com.Tags.{TagclientType, Tagisp, Tagnetwork, Tagprovincecity, TagsAd}
import com.utils.TagUtils
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object redis2idea2 {
  def main(args: Array[String]): Unit = {

    if (args.length != 3) {
      println("目录不匹配，退出程序")
      sys.exit()
    }
    val Array(inputPath, dirPath, stopPath) = args
    // 创建上下文
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)
    // 读取数据
    val df = sQLContext.read.parquet(inputPath)
    // 获取停用词库
    val stopword = sc.textFile(stopPath).map((_, 0)).collectAsMap()
    val bcstopword = sc.broadcast(stopword)

    df.filter(TagUtils.OneUserId)
      .mapPartitions(row =>{
        val jedis = JedisConectionPool.getConection()
        //定义一个可变的 list 集合 空的
        var list = List[(String,List[(String,Int)])]()
        row.map(row =>{
          //取出用户ID
          val userId = TagUtils.getOneUserId(row)
          //  // 接下来通过row数据 打上 所有标签（按照需求）
          val Adlist = TagsAd.makeTags(row)
          val applist = TagApp.makeTags(row)
          val keywordlist = TagstopWord.makeTags(row)
          val clientlist = TagclientType.makeTags(row)
          val isplist = Tagisp.makeTags(row)
          val networklist = Tagnetwork.makeTags(row)
          val locationlist = Tagprovincecity.makeTags(row)
         list:+= (userId,Adlist++applist++keywordlist++clientlist++isplist++networklist++locationlist)
        })
        jedis.close()
        list.iterator
      })
      .reduceByKey((list1,list2)=>
        (list1:::list2)
        .groupBy(_._1)
        .mapValues(_.foldLeft[Int](0)(_+_._2))
        toList
      ).foreach(println)

  }
}