package main.scala1.com.Tags

import com.utils.TagUtils
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 运营商标签
  */
object isp {
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

    df.filter(TagUtils.OneUserId)
      .map(row =>{
        val add = TagUtils.getOneUserId(row)
        val ab = Tagisp.makeTags(row)
        ab
      })
      .foreach(println)

  }
}
