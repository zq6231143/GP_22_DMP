package main.scala1.com.Tags

import com.utils.Tag
import org.apache.spark.sql.Row

object Tagnetwork extends Tag{
  override def makeTags(args: Any*): List[(String, Int)] = {

    var list = List[(String,Int)]()

    val row = args(0).asInstanceOf[Row]

    val networkid = row.getAs[Int]("networkmannerid")
    networkid match {
      case v if v ==3 => list:+=("WIFI D0002000"+v,1)
      case v if v ==4 => list:+=("_   D0002000"+v,1)
      case v if v ==2 => list:+=("3G D0002000"+v,1)
      case v if v ==1 => list:+=("2G D0002000"+v,1)
      case v if v ==5 => list:+=("4G D0002000"+v,1)

    }

    list

  }
}
