package main.scala1.com.Tags

import ch.hsr.geohash.GeoHash
import com.gaode.MapUtil
import com.utils.{Tag, Utils2Type}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row
import redis.clients.jedis.Jedis

/**
  * 商圈标签
  */
object TagBusiness extends Tag {
  override def makeTags(args: Any*): List[(String, Int)] = {

    //先设置结合
    var list = List[(String,Int)]()
    //解析参数
    val row = args(0).asInstanceOf[Row]

    //获取经纬度
    val long = Utils2Type.toDouble(row.getAs[String]("long"))
    val lat = Utils2Type.toDouble(row.getAs[String]("lat"))
    if(long>= 73.0 &&
      long<= 135.0 &&
      lat >=3.0 &&
      lat <=54.0){
      //先去数据库获取商圈
      val business = getBusiness(long,lat)
      //判断缓存中是否有此商圈
      if(StringUtils.isNotBlank(business)){
        val lines = business.split(",")
        lines.foreach(t => list:+=(t,1))
      }
//      list:+=(business,1)
    }
    list
  }

  /**
    * 获取商圈信息
    */
  def getBusiness(long:Double,lat:Double) :String={
    //转换 GeoHash 字符串
   var geohash = GeoHash.geoHashStringWithCharacterPrecision(lat,long,8)
    //去数据库查询
    var business = redis_queryBusiness(geohash)
    //判断商圈是否为空
    if(business ==null || business.length ==0){
      //通过经纬度获取商圈
      business = MapUtil.getBusinessFormMap(long,lat)
      //如果调用高德地图 解析商圈，需要将此次商圈存入 redis
      redis_insertBusiness(geohash,business)
    }
    business
  }


  /**
    * 数据库获取信息
    */
  def redis_queryBusiness(geohash:String): String={
    val jedis = new Jedis("hadoop02",6379)
    val business = jedis.get(geohash)
    jedis.close()
    //返回值
    business
  }

  /**
    * 存储商圈到 redis
    */
  def redis_insertBusiness(geoHash:String, business: String)={
    val jedis = new Jedis("hadoop02",6379)
    jedis.set(geoHash,business)
    jedis.close()
  }


}
