package main.scala1.com.utils

/**
  * 打标签的统一接口
  */
trait Tag {
  //trait 接口
  def makeTags(args:Any*) : List[(String,Int)]

}
