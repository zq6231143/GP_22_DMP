package com.Tags

import com.Appname.{TagApp, TagstopWord}
import com.typesafe.config.ConfigFactory
import com.utils.TagUtils
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 加上 图计算
  */
object TagContext4 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)

    //todo 调用Hbase API
    //加载配置文件
    val load = ConfigFactory.load()
    //    val hbaseTableName = load.getString("hbase.TableName")
    val hbaseTableName = load.getString("hbase.TableName")
//    val hbaseTableName = load.getString("gp22-1")
    //创建 Hadoop 任务
    val configuration = sc.hadoopConfiguration
    configuration.set("hbase.zookeeper.quorum",load.getString("hbase.host"))
//    configuration.set("hbase.zookeeper.quorum","hadoop02:2181")
    //    configuration.set("hbase.zookeeper.quorum",load.getString("hadoop02:2181"))
    //创建 HbaseConnection  Hbase 连接
    val hbconn = ConnectionFactory.createConnection(configuration)
    val hbadmin = hbconn.getAdmin
    //判断表是否可用
    if( ! hbadmin.tableExists(TableName.valueOf(hbaseTableName))){
      //创建表操作
      // val tableDescriptor = new HTableDescriptor(TableName.valueOf(hbaseTableName))
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
    val baseRDD: RDD[(List[String], Row)] = df.filter(TagUtils.OneUserId)
      //剩下的标签在内部实现
      .map(row => {
        val userlist = TagUtils.getAllUserId(row)
        (userlist, row)
    })

      //构建点集合
     val vertiesRDD:RDD[(Long, List[(String, Int)])] = baseRDD.flatMap(tp =>{
        val row = tp._2
        //所有标签
        val adlist = TagsAd.makeTags(row)
        val applist = TagApp.makeTags(row, broadcast)
        val keywordlist = TagstopWord.makeTags(row, bcstopword)
        val devicelist = TagDevice.makeTags(row)
        val locationlist = Tagprovincecity.makeTags(row)
        val businesslist = TagBusiness.makeTags(row)

        val AllTag = adlist ++ applist ++ keywordlist ++ devicelist ++ locationlist ++ businesslist
      //List((String,Int))
        //保证其中一个点携带着所有标签，同时也保留所有的 userId
        val VD = tp._1.map((_,0)) ++ AllTag
        //处理所有的点的集合
        tp._1.map(uId =>{
          //保证一个点携带标签(uid,vd),(uid,list()),(uid,list())
          if(tp._1.head.equals(uId)){
            (uId.hashCode.toLong,VD)
          }else{
            (uId.hashCode.toLong, List.empty)
          }
        })
      })

    // vertiesRDD.take(50).foreach(println)
     //构建边的集合
 val edges: RDD[Edge[Int]] = baseRDD.flatMap(tp => {
   //A B C : A->B A->C
   tp._1.map(uId => Edge(tp._1.head.hashCode, uId.hashCode, 0))
 })

    //构建图
val graph: Graph[List[(String, Int)], Int] = Graph(vertiesRDD, edges)
  //取出顶点 使用的是图计算中的连通图算法
    val vertices = graph.connectedComponents().vertices
    //处理所有的标签和 id
      vertices.join(vertiesRDD).map{
        case (uId, (conId, tagsAll)) => (conId, tagsAll)
      }.reduceByKey((list1,list2) =>{
        //聚合所有的标签
        (list1 ++ list2).groupBy(_._1).mapValues(_.map(_._2).sum).toList
      })
//      .take(20).foreach(println)

        .map{
      case(userid,userTag) =>{
        val put = new Put(Bytes.toBytes(userid))
        //处理下标签
        val tags = userTag.map(t => t._1 +","+ t._2).mkString(",")
        put.addImmutable(Bytes.toBytes("tags"), Bytes.toBytes("$20190827"),Bytes.toBytes(tags))
        (new ImmutableBytesWritable(),put)
      }
    }.saveAsHadoopDataset(jobConf)

    sc.stop()
  }
}
