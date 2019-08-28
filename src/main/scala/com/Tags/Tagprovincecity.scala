package com.Tags

import com.utils.Tag
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.Row

/**
  * 地域标签
  */
object Tagprovincecity extends Tag {
  override def makeTags(args: Any*): List[(String, Int)] = {

    var list = List[(String,Int)]()

    val row = args(0).asInstanceOf[Row]

    val province = row.getAs[String]("provincename")
    if(StringUtils.isNotBlank(province)){
      list:+=("ZP"+province,1)
    }
    list

    val city = row.getAs[String]("cityname")
    if(StringUtils.isNotBlank("cityname")){
      list:+=("ZC"+city,1)
    }
    list
  }
}
