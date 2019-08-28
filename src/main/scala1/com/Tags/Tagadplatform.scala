package main.scala1.com.Tags

import com.utils.Tag
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

object Tagadplatform extends Tag{
  override def makeTags(args: Any*): List[(String, Int)] = {

    //定义一个变长数组   空的
    var list = List[(String,Int)]()
    //解析参数
    val row = args(0).asInstanceOf[Row]
    //获取 渠道 名字    
     val adplatType = row.getAs[Int]("adplatformproviderid")
    adplatType match {
      case v if v >=100000  => list:+=("CN"+v,1)
    }
    val adplatname = row.getAs[String]("adplatformkey")
    if(StringUtils.isNotBlank(adplatname)){
      list:+=("CNname"+adplatname,1)
    }
      list
  }
}
