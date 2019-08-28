package com.utils

/**
  * 数据类型转换
  */
object Utils2Type {

  //String 转换 Int
  def toInt(str:String) :Int ={
    //转换不了，出异常，为0
    try{
      str.toInt
    }catch{
      case _:Exception =>0
    }
  }

  //String 转换 Double
  def toDouble(str:String) :Double ={
    //转换不了，出异常，为0
    try{
      str.toDouble
    }catch{
      case _:Exception =>0.0
    }
  }
}
