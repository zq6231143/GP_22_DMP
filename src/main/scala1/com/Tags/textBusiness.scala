package main.scala1.com.Tags

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 测试商圈标签
  */
object textBusiness {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[2]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val df: DataFrame = sqlContext.read.parquet("G:/all")
    df.map(row =>{
      val business = TagBusiness.makeTags(row)
      business
    }).foreach(println)

  }
}
