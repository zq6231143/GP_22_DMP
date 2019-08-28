package main.scala1.com.Tags

import com.utils.Tag
import org.apache.spark.sql.Row

object Tagisp extends Tag{
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()

    val row = args(0).asInstanceOf[Row]

    val ispid = row.getAs[Int]("ispid")
    ispid match {
      case v if v ==1 =>list:+=("移 动 D0003000"+v,1)
      case v if v ==2 =>list:+=("联 通 D0003000"+v,1)
      case v if v ==3 =>list:+=("电 信 D0003000"+v,1)
      case v if v ==4 =>list:+=("_ D0003000"+v,1)
    }

    list

  }
}
