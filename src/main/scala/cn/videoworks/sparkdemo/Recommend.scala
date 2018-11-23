package cn.videoworks.sparkdemo

import java.io.FileInputStream
import java.util.Properties

import cn.videoworks.sparkdemo.ModelTraining.{appName, hbaseMaster, putDataTable, hbaseZookeeperPort, hbaseZookeeperQuorum, hdfsDir, hdfsRatingsFileName, hdfsRatingsSourceDir, hdfsUrl, recommendCount, sparkMaster, sparkOtherConfig_numThreads, yesterdayYMD, _}
import cn.videoworks.sparkdemo.conf.AppConfig
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, HBaseAdmin, Table}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.recommendation.ALSModel
import org.apache.spark.sql.DataFrame

import scala.collection.mutable

/**
  * 推荐
  */
object Recommend extends AppConfig {


  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.WARN)
    //获取配置
    loadConfig(args)

    val path = hdfsUrl + hdfsDir + yesterdayYMD()
    val loadModel = ALSModel.load(path)
    println("开始为所有用户推荐3部电影 只显示 20条展示")
    val userRecs: DataFrame = loadModel.recommendForAllUsers(recommendCount)
    //    val df = userRecs.withColumn("recommendations", userRecs("recommendations").getField("movieId").cast(StringType)).withColumnRenamed("recommendations", "movieIds")
    userRecs.show(20, false)

    //

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
        val arrs = userRecs.collect()
        arrs.map(row => {
          val userId = row.getInt(0)
          val countWArr: mutable.WrappedArray[Any] = row.get(1).asInstanceOf[mutable.WrappedArray[Any]]
          putDataTable(userId, countWArr, table,"recommends")
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
