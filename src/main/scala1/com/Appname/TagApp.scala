package main.scala1.com.Appname

import com.utils.Tag
import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

object TagApp extends Tag {
  override def makeTags(args: Any*): List[(String, Int)] = {

    var list = List[(String,Int)]()
    //解析参数类型
    val row = args(0).asInstanceOf[Row]
    val appMap = args(1).asInstanceOf[Broadcast[Map[String, String]]]  //广播变量

    val appid = row.getAs[String]("appid")
    val appname = row.getAs[String]("appname")
    //空值判断
    if(StringUtils.isNotBlank(appname)){
      list:+=("App"+appname,1)
    }else if(StringUtils.isNoneBlank(appid)){
      list:+=("APP"+appMap.value.getOrElse(appid,"Null"),1)
    }
    list
  }
}
