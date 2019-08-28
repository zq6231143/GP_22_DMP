package com.utils

/**
  * 做指标方法
  */
object RptUtils {

  //此方法处理请求书
  def request(requestmode: Int,processnode:Int) :List[Double]={
  //现在这个list里面包含三个指标 list(1,1,1,) 三个元素  第一个原始请求数，第二个有效请求书，第三个广告请求数
    if(requestmode==1 && processnode ==1){
      List[Double](1,0,0)
    }else if(requestmode==1 && processnode ==2){
      List[Double](1,1,0)
    }else if(requestmode==1 && processnode ==3){
      List[Double](1,1,1)
    }else{
      List[Double](0,0,0)
    }
  }

  //此方法处理展示点击数
  def click(requestmode: Int,iseffective:Int) : List[Double]={
    if(requestmode==2 && iseffective ==1){
      List[Double](1,0)
    }else if(requestmode==3 && iseffective ==1){
      List[Double](1,1)
    }else{
      List[Double](0,0)
    }
  }

  //此方法处理竞价操作
  def Ad(iseffective:Int,isbilling:Int,isbid:Int,iswin:Int,adorderid:Int,
         winprice:Double,adpayment:Double): List[Double] ={
    if(iseffective ==1 && isbilling ==1 && isbid==1){
      if(iseffective ==1 && isbilling ==1 && iswin ==1 && adorderid !=0){
        List[Double](1,1,winprice/1000.0,adpayment/1000.0)
      }else{
        List[Double](1,0,0,0)
      }
    }else{
      List[Double](0,0,0,0)
    }
  }

  //处理 设备类
  //设备类型（1：手机 2：平板）
  def deviceTyple(devicetype:Int) :String ={
    if(devicetype ==1){
     "手机"
    }else if(devicetype ==2){
      "平板"
    }else{
      "其他"
    }
  }

  //处理 操作系
  //1：android 2：ios 3：wp
  def androidAndios(client:Int) : String={
    if(client ==1){
      "android"
    }else if(client ==2){
      "ios"
    }else{
      "其他"
    }
  }


  //媒体分析指标



}
