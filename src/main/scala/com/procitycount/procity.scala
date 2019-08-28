package com.procitycount

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

/**
  *求需求指标2 统计各省各市（地域）数据分布
  */
object procity {
  def main(args: Array[String]): Unit = {

    //判断路径是否正确
    if(args.length !=1 ){
      println("目录参数不正确，退出程序")
      sys.exit()
    }
    //创建一个集合保存输入输出目录
    val Array(inputPath) = args
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
      //设置序列化方式，采用 Kyro 序列化方式，比默认序列化方式高效
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    //创建执行入口
    val sc = new SparkContext(conf)
    //采用 parquet 格式存储
    val sqlContext = new SQLContext(sc)
    //改变压缩方式---存储的时候压缩,使用Snappy方式进行压缩(比率小，速度慢)
    sqlContext.setConf("spark.sql.parquet.compression.codec","snappy")
    //进行数据的读取，处理分析数据
    val dataFrame = sqlContext.read.parquet(inputPath)
    //注册临时表（1.6）   2.0 以后--临时试图
    dataFrame.registerTempTable("t_tmp")
    val result = sqlContext.sql("select provincename, cityname, count(1) from t_tmp group by provincename, cityname")



//    result.write.json("G:/outputPath-20190821-1")
   // result.coalesce(1).write.partitionBy("provincename","cityname").json("G:/outputPath-20190821-2")
   // result.coalesce(10).write.partitionBy("provincename","cityname").json("hdfs://hadoop02:8020/outputPath-20190821")

    //写入本地 mysql57
    val prop = new Properties()
    prop.put("user","root")
    prop.put("password","123456")
    prop.put("url","jdbc:mysql://localhost:3306/spark01")
    result.write.mode(SaveMode.Append).jdbc("jdbc:mysql://localhost:3306/spark01","procitycount",prop)
    sc.stop()
  }
}
