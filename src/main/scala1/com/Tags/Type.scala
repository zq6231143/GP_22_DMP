package main.scala1.com.Tags

import com.utils.TagUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * 设备标签
  */
object Type {
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

    df.filter(TagUtils.OneUserId)
      .map(row =>{
        val tup = TagUtils.getOneUserId(row)
        val aa = TagclientType.makeTags(row)
        aa
      })
      .foreach(println)
  }
}
