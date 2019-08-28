package com.redis_idea

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 将字典文件数据存储到 redis
  */
object APP2Redis {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val sc = new SparkContext(conf)

    val dir = sc.textFile("G:\\种子\\spark项目\\项目day01\\Spark用户画像分析\\app_dict.txt")

    dir.map(_.split("\t",-1))
        .filter(_.length >=5).foreachPartition(arr =>{
      val jedis = JedisConectionPool.getConection()
      arr.foreach(arr =>{
        jedis.set(arr(4),arr(1))
      })
      jedis.close()
    }
    )
    sc.stop()
  }
}
