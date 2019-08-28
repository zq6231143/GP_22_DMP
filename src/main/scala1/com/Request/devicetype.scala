package main.scala1.com.Request

import com.utils.RptUtils
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 设备类
  */
object devicetype {
  def main(args: Array[String]): Unit = {

    //判断路径是否正确
    if(args.length != 1){
      println("目录参数不正确，退出程序")
      sys.exit()
    }
    //创建一个集合保存输入和输出目录
    val Array(inputPath) = args
    //初始化
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
      //设置序列化方式，采用 Kyro 序列化方式，比默认序列化方式高效
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    //创建执行入口
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val df = sqlContext.read.parquet(inputPath)
    df.registerTempTable("tmp")

    df.map(row => {
      //取需要的字段
      val requestmode = row.getAs[Int]("requestmode")
      val processnode = row.getAs[Int]("processnode")
      val iseffective = row.getAs[Int]("iseffective")
      val isbilling = row.getAs[Int]("isbilling")
      val isbid = row.getAs[Int]("isbid")
      val iswin = row.getAs[Int]("iswin")
      val adorderid = row.getAs[Int]("adorderid")

      val winprice = row.getAs[Double]("winprice")
      val adpayment = row.getAs[Double]("adpayment")

      //获取 k
      val devicetype = row.getAs[Int]("devicetype")

      //获取 9个字段
      val requestlist = RptUtils.request(requestmode, processnode)
      val clicklist = RptUtils.click(requestmode, iseffective)
      val Adlist = RptUtils.Ad(iseffective, isbilling, isbid, iswin, adorderid, winprice, adpayment)
      (RptUtils.deviceTyple(devicetype), requestlist ++ clicklist ++ Adlist)
    })
      .reduceByKey((list1,list2)=>{
        list1.zip(list2).map(x => x._1 + x._2)
      }).map(x =>{x._1 +","+ x._2.mkString(",")})
      .saveAsTextFile("G:/outputPath/output20190822-4")




//    val frame = sqlContext.sql("select devicetype," +
//      "max(case when requestmode =1 and processnode >= 1 then 1 else 0 end) t1," +
//      "max(case when requestmode =1 and processnode >= 2 then 1 else 0 end) t2," +
//      "max(case when requestmode =1 and processnode =3 then 1 else 0 end) t3," +
//      "max(case when iseffective =1 and isbilling =1 and isbid =1 then 1 else 0 end) t4," +
//      "max(case when iseffective =1 and isbilling =1 and iswin =1 and adorderid !=0 then 1 else 0 end)t5," +
//      "max(case when requestmode =2 and iseffective =1 then 1 else 0 end) t6,"+
//      "max(case when requestmode =3 and iseffective =1 then 1 else 0 end) t7," +
//      "max(case when iseffective =1 and isbilling =1 and iswin =1 then 1 else 0 end) t8," +
//      "max(case when iseffective =1 and isbilling =1 and iswin =1 then 1 else 0 end) t9 from tmp group by devicetype")
//
//    frame.show()

  }
}
