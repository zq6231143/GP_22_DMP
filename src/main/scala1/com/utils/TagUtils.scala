package main.scala1.com.utils

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

/**
  * 标签工具类
  *object class 区别
  * object 相当于创建 单例对象
  */
object TagUtils {

  //用户唯一不为空 id
  //先去过滤需要的字段
  val OneUserId =
  """
    | imei !='' or mac !='' or openudid !='' or androidid !='' or idfa !='' or
    | imeimd5 !='' or macmd5 !='' or openudidmd5 !='' or androididmd5 !='' or idfamd5 !='' or
    | imeisha1 !='' or macsha1 !='' or openudidsha1 !='' or androididsha1 !='' or idfasha1 !=''
  """.stripMargin

  //取出唯一不为空的 ID
  def getOneUserId(row:Row):String={
    row match {
        //模式匹配
      case v if StringUtils.isNotBlank(v.getAs[String]("imei")) => "IM: "+v.getAs[String]("imei")
      case v if StringUtils.isNotBlank(v.getAs[String]("mac")) => "MAC: "+v.getAs[String]("mac")
      case v if StringUtils.isNotBlank(v.getAs[String]("openudid")) => "OPD: "+v.getAs[String]("openudid")
      case v if StringUtils.isNotBlank(v.getAs[String]("androidid")) => "AND: "+v.getAs[String]("androidid")
      case v if StringUtils.isNotBlank(v.getAs[String]("idfa")) => "IDF: "+v.getAs[String]("idfa")
      case v if StringUtils.isNotBlank(v.getAs[String]("imeimd5")) => "IMM: "+v.getAs[String]("imeimd5")
      case v if StringUtils.isNotBlank(v.getAs[String]("macmd5")) => "MACM: "+v.getAs[String]("macmd5")
      case v if StringUtils.isNotBlank(v.getAs[String]("openudidmd5")) => "OPDD: "+v.getAs[String]("openudidmd5")
      case v if StringUtils.isNotBlank(v.getAs[String]("androididmd5")) => "ANDD: "+v.getAs[String]("androididmd5")
      case v if StringUtils.isNotBlank(v.getAs[String]("idfamd5")) => "IDFD: "+v.getAs[String]("idfamd5")
      case v if StringUtils.isNotBlank(v.getAs[String]("imeisha1")) => "IMS: "+v.getAs[String]("imeisha1")
      case v if StringUtils.isNotBlank(v.getAs[String]("macsha1")) => "MACS: "+v.getAs[String]("macsha1")
      case v if StringUtils.isNotBlank(v.getAs[String]("openudidsha1")) => "OPDS: "+v.getAs[String]("openudidsha1")
      case v if StringUtils.isNotBlank(v.getAs[String]("androididsha1")) => "ANDS: "+v.getAs[String]("androididsha1")
      case v if StringUtils.isNotBlank(v.getAs[String]("idfasha1")) => "IDFS: "+v.getAs[String]("idfasha1")
    }
  }

  //获取所有 ID
  def getAllUserId(row :Row): List[String] ={
    var list = List[String]()
    if(StringUtils.isNotBlank(row.getAs[String]("imei"))) list:+= "IM :" + row.getAs[String]("imei")
    if(StringUtils.isNotBlank(row.getAs[String]("mac"))) list:+= "MAC: "+row.getAs[String]("mac")
    if(StringUtils.isNotBlank(row.getAs[String]("openudid"))) list:+= "OPD: "+row.getAs[String]("openudid")
    if(StringUtils.isNotBlank(row.getAs[String]("androidid"))) list:+= "AND: "+row.getAs[String]("androidid")
    if(StringUtils.isNotBlank(row.getAs[String]("idfa"))) list:+= "IDF: "+row.getAs[String]("idfa")
    if(StringUtils.isNotBlank(row.getAs[String]("imeimd5"))) list:+=  "IMM: "+row.getAs[String]("imeimd5")
    if(StringUtils.isNotBlank(row.getAs[String]("macmd5"))) list:+=  "MACM: "+row.getAs[String]("macmd5")
    if(StringUtils.isNotBlank(row.getAs[String]("openudidmd5")))list:+=  "OPDD: "+row.getAs[String]("openudidmd5")
    if(StringUtils.isNotBlank(row.getAs[String]("androididmd5"))) list:+= "ANDD: "+row.getAs[String]("androididmd5")
    if(StringUtils.isNotBlank(row.getAs[String]("idfamd5"))) list:+= "IDFD: "+row.getAs[String]("idfamd5")
    if(StringUtils.isNotBlank(row.getAs[String]("imeisha1")))list:+=  "IMS: "+row.getAs[String]("imeisha1")
    if(StringUtils.isNotBlank(row.getAs[String]("macsha1"))) list:+= "MACS: "+row.getAs[String]("macsha1")
    if(StringUtils.isNotBlank(row.getAs[String]("openudidsha1"))) list:+=  "OPDS: "+row.getAs[String]("openudidsha1")
    if(StringUtils.isNotBlank(row.getAs[String]("androididsha1"))) list:+=  "ANDS: "+row.getAs[String]("androididsha1")
    if(StringUtils.isNotBlank(row.getAs[String]("idfasha1"))) list:+=  "IDFS: "+row.getAs[String]("idfasha1")
    list
  }
}
