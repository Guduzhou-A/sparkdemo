package cn.videoworks.sparkdemo

import cn.videoworks.sparkdemo.ModelTraining._
import cn.videoworks.sparkdemo.Recommend.{initTable => _, loadConfig => _, parseListen => _, putDataTable => _, _}
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

/**
  * listen
  * 计算用户推荐
  */
object ListenRecommend {

  def main(args: Array[String]): Unit = {
    //设置log基本，生产也建议使用WARN
    Logger.getRootLogger.setLevel(Level.WARN)

    //读取用户听歌内容
    //获取外部配置文件
    loadConfig(args)

    //    创建sparkSession
    val sparkConf = new SparkConf().setAppName(appName).setMaster(sparkMaster).set("spark.rpc.netty.dispatcher.numThreads", sparkOtherConfig_numThreads)
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    //导入数据
    println("开始获取用户听歌数据.....")
    val fileRegex = "/part-*"
    val ratingsData = spark.read.textFile(hdfsUrl + hdfsListenDir + hdfsListenFileName + fileRegex).rdd
    val parts = ratingsData.map(line => parseListen(line))
    val allDatas = spark.createDataFrame(parts).cache()


    println("收集用户听歌数据.....")

    println("开始..导入hbase.....")
    val confhbase = HBaseConfiguration.create()
    confhbase.set("hbase.zookeeper.property.clientPort", hbaseZookeeperPort)
    confhbase.set("hbase.zookeeper.quorum", hbaseZookeeperQuorum)
    confhbase.set("hbase.master", hbaseMaster)
    val connection = ConnectionFactory.createConnection(confhbase)
    val userTable = TableName.valueOf(hbaseListenTableName)
    //初始化表单
    initTable(connection, userTable)
    val table = connection.getTable(userTable)
    try {
      try {
        val arrs = allDatas.collect()
        arrs.map(row => {
          val userId = row.getInt(0)
          val ratingWArr: mutable.WrappedArray[Any] = row.get(1).asInstanceOf[mutable.WrappedArray[Any]]
          putDataTable(userId, ratingWArr, table, "listenCountInfo")
        })

      } finally {
        if (table != null) table.close()
      }
    }
    finally {
      connection.close()
    }
    println("导入hbase ... 结束")


  }

}
