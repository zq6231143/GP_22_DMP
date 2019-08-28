package com.redis_idea

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis
/**
  * 把媒体数据  字典写入redis 中
  */
object redis_idea {
  def main(args: Array[String]): Unit = {

//    if(args.length != 2){
//      println("目录路径不对，请退出程序")
//      sys.exit()
//    }
//    val Array(inputPath,dirPath) = args
    //创建上下文
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
      //设置序列化方式，采用 Kyro 序列化方式，比默认序列化方式高效
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    //读取数据
//    val df: DataFrame = sqlContext.read.parquet(inputPath)

    //获取 redis 对象，连接
//    val redis = new Jedis("192.168.19.100",6379)
//    val map2 = sc.textFile("G:/种子/spark项目/项目day01/Spark用户画像分析/2016-10-01_06_p1_invalid.1475274123982.log")
//      .map(_.split("\t",-1)).filter(_.length<=5)
//      .map(arr=>{(arr(4),arr(1))})
//      .foreach(tup => redis.set(tup._1,tup._2))


    //字典数据

    val map1 = sc.textFile("G:/种子/spark项目/项目day01/Spark用户画像分析/app_dict.txt")
      .map(_.split("\t",-1)).filter(_.length<=5)
      .map(arr=>{(arr(4),arr(1))})
      .foreachPartition(itr=>{
        val redis = new Jedis("192.168.149.101",6379)
        itr.map(x=>{
          redis.set(x._1,x._2)
        })
      })





  }
}
