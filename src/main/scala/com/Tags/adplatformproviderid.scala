package com.Tags

import com.utils.TagUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * 渠道标签
  */
object adplatformproviderid {
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

    val line = df.filter(TagUtils.OneUserId)
      .map(row =>{
        //取出id
        val adplatid = TagUtils.getOneUserId(row)
        val adlist = Tagadplatform.makeTags(row)
        adlist
      })
      .collect.toBuffer.foreach(println)




  }
}
