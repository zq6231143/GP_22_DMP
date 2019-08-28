package com.Appname

import com.Tags.TagsAd
import com.utils.TagUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * APP--2)	App 名称（标签格式： APPxxxx->1）xxxx 为 App 名称
  */
object Appname {
  def main(args: Array[String]): Unit = {
    if(args.length != 3){
      println("目录路径不对，请退出程序")
      sys.exit()
    }
    val Array(inputPath,dirPath,stopPath) = args
    //创建上下文
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    //读取数据
    val df: DataFrame = sqlContext.read.parquet(inputPath)

    val map: collection.Map[String, String] = sc.textFile(dirPath).map(_.split("\t", -1)).filter(_.length >= 5)
      .map(arr => (arr(4), arr(1))).collectAsMap()

    //广播变量
    val broadcast = sc.broadcast(map)
    //获取停用字库
    val stopword = sc.textFile(stopPath).map((_,0)).collectAsMap()
    //广播一下
    val wordstop = sc.broadcast(stopword)

    //过滤
    df.filter(TagUtils.OneUserId)
      .map(row =>{
        val id = TagUtils.getOneUserId(row)
        val aa = TagsAd.makeTags(row)
        val applist = TagApp.makeTags(row,broadcast)
        val keyword = TagstopWord.makeTags(row,wordstop)
//        applist
        keyword
      })
      .foreach(println)

  }
}
