package com.Appname

import com.utils.Tag
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row


/**
  * 关键字标签
  */
object TagstopWord extends Tag {
  override def makeTags(args: Any*): List[(String, Int)] = {

    var list = List[(String,Int)]()

    val row = args(0).asInstanceOf[Row]
    val stopword = args(1).asInstanceOf[Broadcast[Map[String, String]]]
    //获取关键字，打标签
    val keyword = row.getAs[String]("keywords").split("\\|")
    //按照条件过滤数据
    keyword.filter(word => {
      word.length >= 3 && word.length <= 8 && !stopword.value.contains(word)
    })
      .foreach(word => list:+=("K"+word,1))
    list
  }
}
