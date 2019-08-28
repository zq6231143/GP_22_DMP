package main.scala1.com.Tags

import com.utils.Tag
import org.apache.spark.sql.Row

object TagclientType extends Tag{
  override def makeTags(args: Any*): List[(String, Int)] = {

    var list = List[(String,Int)]()

    val row = args(0).asInstanceOf[Row]

    val clientname = row.getAs[Int]("client")
    clientname match {
      case v if v ==1 =>list:+=("Android D0001000"+v,1)
      case v if v ==2 =>list:+=("IOS D0001000"+v,1)
      case v if v ==3 =>list:+=("WinPhone D0001000"+v,1)
      case v if v==4 => list:+=("其 他 D0001000"+v,1)
    }

//    val devicename = row.getAs[String]("device")
//    if(StringUtils.isNotBlank("devicename")){
//      list:+=("型号"+devicename,1)
//    }
    list
  }
}
