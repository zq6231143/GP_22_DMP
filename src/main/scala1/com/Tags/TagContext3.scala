package main.scala1.com.Tags

import com.Appname.{TagApp, TagstopWord}
import com.typesafe.config.ConfigFactory
import com.utils.TagUtils
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object TagContext3 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)

    //todo 调用Hbase API
    //加载配置文件
    val load = ConfigFactory.load()
//    val hbaseTableName = load.getString("hbase.TableName")
    val hbaseTableName = "gp22"
    //创建 Hadoop 任务
    val configuration = sc.hadoopConfiguration
    configuration.set("hbase.zookeeper.quorum","hadoop02:2181")
//    configuration.set("hbase.zookeeper.quorum",load.getString("hadoop02:2181"))
    //创建 HbaseConnection  Hbase 连接
    val hbconn = ConnectionFactory.createConnection(configuration)
    val hbadmin = hbconn.getAdmin
    //判断表是否可用
    if( ! hbadmin.tableExists(TableName.valueOf(hbaseTableName))){
      //创建表操作
//      val tableDescriptor = new HTableDescriptor(TableName.valueOf(hbaseTableName))
        val tableDescriptor = new HTableDescriptor(TableName.valueOf(hbaseTableName))
    //创建 列簇
      val descriptor = new HColumnDescriptor("tags")
      //将列簇加入表中, 把表加载到 admin
      tableDescriptor.addFamily(descriptor)
      hbadmin.createTable(tableDescriptor)
      //关闭
      hbadmin.close()
      hbconn.close()
    }

    //创建 JobConf
    val jobConf = new JobConf(configuration)
    //指定输出类型 key 和 指定表
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE ,hbaseTableName)


    // 读取数据
    val df = sQLContext.read.parquet("G:/outputPath-20190820")

    //字典文件
    val map = sc.textFile("G:\\种子\\spark项目\\项目day01\\Spark用户画像分析\\app_dict.txt")
      .map(_.split("\t", -1))
      .filter(_.length >= 5).map(arr => (arr(4), arr(1))).collectAsMap()
    // 将处理好的数据广播
    val broadcast = sc.broadcast(map)

    // 获取停用词库
    val stopword = sc.textFile("G:\\种子\\spark项目\\项目day01\\Spark用户画像分析\\stopwords.txt").map((_, 0)).collectAsMap()
    val bcstopword = sc.broadcast(stopword)

    //过滤符合条件的数据 ID
    df.filter(TagUtils.OneUserId)
      //剩下的标签在内部实现
      .map(row=> {
        //取出用户ID
        val userId = TagUtils.getOneUserId(row)
        // 接下来通过row数据 打上 所有标签（按照需求）
        val adlist = TagsAd.makeTags(row)
        val applist = TagApp.makeTags(row, broadcast)
        val keywordlist = TagstopWord.makeTags(row, bcstopword)
        val devicelist = TagDevice.makeTags(row)
        val locationlist = Tagprovincecity.makeTags(row)
        val businesslist = TagBusiness.makeTags(row)

    (userId, adlist++applist++keywordlist++devicelist++locationlist++businesslist)
      })
        .reduceByKey((list1,list2)=>
          // List(("lN插屏",1),("LN全屏",1),("ZC沈阳",1),("ZP河北",1)....)
          (list1:::list2)
            // List(("APP爱奇艺",List()))
            .groupBy(_._1)
            .mapValues(_.foldLeft[Int](0)(_+_._2))
            .toList
        ).map{
      case(userid,userTag) =>{
        val put = new Put(Bytes.toBytes(userid))
        //处理下标签
        val tags = userTag.map(t => t._1 +","+ t._2).mkString(",")
        put.addImmutable(Bytes.toBytes("tags"), Bytes.toBytes("$20190826"),Bytes.toBytes(tags))
        (new ImmutableBytesWritable(),put)
      }
    }.saveAsHadoopDataset(jobConf)


}

}
