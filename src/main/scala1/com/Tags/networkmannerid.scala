package main.scala1.com.Tags

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 设备联网方式标签
  */
object networkmannerid {
  def main(args: Array[String]): Unit = {
    if(args.length != 1){
      println("目录路径不对，请退出程序")
      sys.exit()
    }
    val Array(inputPath) = args
    //创建上下文
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val sc = new SparkContext(conf)

    //读取数据
    val df = sc.textFile("C:/Users/zhangqiao/Desktop/项目第一周考试/json.txt")
    //过滤符合条件的 userid
    df.map(_.split(","))
      .map(row => {
      //接下来通过 row 数据 打上所有标签
      val network = Tagnetwork.makeTags(row)

      network
    })
    .foreach(println)

  }
}
