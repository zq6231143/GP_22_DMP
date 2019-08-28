package com.Tags

import com.utils.Tag
import org.apache.spark.sql.Row

/**
  * 设备标签
  */
object TagDevice extends Tag{
  override def makeTags(args: Any*): List[(String, Int)] = {
    //创建集合，用来接收数据
    var list = List[(String,Int)]()   //后面的小括号 是方法
    //处理参数
    val row = args(0).asInstanceOf[Row]
    //设备类型
    val client = row.getAs[Int]("client")
    client match {
      case 1 =>list:+=("Android D00010001",1)
      case 2 =>list:+=("IOS D00010002",1)
      case 3 =>list:+=("WinPhone D00010003",1)
      case _ => list:+=("其 他 D00010004",1)
    }
    //联网方式
    val network = row.getAs[String]("networkmannername")
    network match {
      case "Wifi" =>list:+=("Wifi D00020001",1)
      case "4G" =>list:+=("4G D00020002",1)
      case "3G" =>list:+=("3G D00020003",1)
      case "2G" =>list:+=("2G D00020004",1)
      case _ =>list:+=("其他 D00020005",1)
    }

    //运营商
    val isp = row.getAs[String]("ispname")
    isp match {
      case "移动" =>list:+=("移动 D00030001",1)
      case "联通" =>list:+=("联通 D00030002",1)
      case "电信" =>list:+=("电信 D00030003",1)
      case _ =>list:+=("其他 D00030004",1)
    }
    //返回值  空的
    list
  }
}
