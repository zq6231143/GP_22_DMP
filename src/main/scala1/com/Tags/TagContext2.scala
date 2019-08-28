package main.scala1.com.Tags

import com.Appname.{TagApp, TagstopWord}
import com.redis_idea.JedisConectionPool
import com.utils.TagUtils
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object TagContext2 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)
    // 读取数据
    val df = sQLContext.read.parquet("G:/outputPath-20190820")

//    //字典文件
//    val map = sc.textFile("G:\\种子\\spark项目\\项目day01\\Spark用户画像分析\\app_dict.txt")
//      .map(_.split("\t", -1))
//      .filter(_.length >= 5).map(arr => (arr(4), arr(1))).collectAsMap()
//    // 将处理好的数据广播
//    val broadcast = sc.broadcast(map)

    // 获取停用词库
    val stopword = sc.textFile("G:\\种子\\spark项目\\项目day01\\Spark用户画像分析\\stopwords.txt").map((_, 0)).collectAsMap()
    val bcstopword = sc.broadcast(stopword)

    //过滤符合条件的数据 ID
    df.filter(TagUtils.OneUserId)
      //剩下的标签在内部实现
      .mapPartitions(row=> {
      val jedis = JedisConectionPool.getConection()
      var list = List[(String, List[(String, Int)])]()
      row.map(row =>{
        //取出用户ID
        val userId = TagUtils.getOneUserId(row)
        // 接下来通过row数据 打上 所有标签（按照需求）
        val adlist = TagsAd.makeTags(row)
        val applist = TagApp.makeTags(row, jedis)
        val keywordlist = TagstopWord.makeTags(row, bcstopword)
        val devicelist = TagDevice.makeTags(row)
        val locationlist = Tagprovincecity.makeTags(row)

        list:+=(userId, adlist++applist++keywordlist++devicelist++locationlist)
      })
      jedis.close()
      list.iterator
         })
      .reduceByKey((list1, list2) =>
        // List(("lN插屏",1),("LN全屏",1),("ZC沈阳",1),("ZP河北",1)....)
        (list1 ::: list2)
          //相当于拼接  list1，list2集合中的数据全部方放到一个集合里，一个新的结合,对新集合进行操作
          //List(("APP爱奇艺",List()))
          .groupBy(_._1)
          .mapValues(_.foldLeft[Int](0)(_ + _._2)) //mapValues(_.size)也可以
          .toList //转换成 list
      ).foreach(println)

  }
}