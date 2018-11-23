package cn.videoworks.sparkdemo.conf

import java.io.FileInputStream
import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Properties}

import cn.videoworks.sparkdemo.model.{Listen, Rating, Sing}
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema

import scala.collection.mutable

trait AppConfig {

  var appName = "spark demo"
  var sparkMaster = "spark://hadoop-master:7077"
  var sparkOtherConfig_numThreads = "2"

  var hdfsUrl = "hdfs://dtns:8020/"
  var hdfsDir = "model/"

  //rating
  var hdfsRatingsSourceDir = "rcmd/rating/song/"
  var hdfsRatingsFileName = ""

  //listen
  var hdfsListenDir = "rcmd/listen/"
  var hdfsListenFileName = ""

  //sing
  var hdfsSingDir = "rcmd/listen/"
  var hdfsSingFileName = ""

  var hbaseZookeeperPort = "2181"
  var hbaseZookeeperQuorum = "slave-01,slave-02,slave-03"
  var hbaseMaster = "hadoop-master:60000"

  var hbaseRecommendTableName = "recommend_als"
  var recommendCount = 5

  var hbaseListenTableName = "recommend_listen"

  var hbaseSingTableName = "recommend_sing"


  var todayYMD = new SimpleDateFormat("yyyy-MM-dd").format(new Date())


  def yesterdayYMD(): String = {
    val cal = Calendar.getInstance()
    cal.setTime(new Date())
    cal.add(Calendar.DATE, -1)
    new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
  }

  def parseRating(str: String): Rating = {
    val fields = str.split("|")
    //    assert(fields.size == 3)
    Rating(fields(0).toInt, fields(1).toInt, fields(2).toFloat)
  }

  def parseListen(str: String): Listen = {
    val fields = str.split("|")
    Listen(fields(0).toInt, fields(1).toInt, fields(2).toInt)
  }

  def parseSing(str: String): Sing = {
    val fields = str.split("|")
    Sing(fields(0).toInt, fields(1).toInt, fields(2).toInt)
  }

  def putDataTable(userId: Int, vArr: mutable.WrappedArray[Any], t: Table, rowName: String) = {
    val userIdKey = "userId:" + userId
    val p = new Put(Bytes.toBytes(userIdKey))
    val ratingArr = vArr.array
    ratingArr.foreach(row => {
      val rowObj = row.asInstanceOf[GenericRowWithSchema]
      val key = rowObj.get(0)
      val value = rowObj.get(1)
      p.addColumn(Bytes.toBytes(rowName), Bytes.toBytes(key.toString), Bytes.toBytes(value.toString))
    })
    t.put(p)
  }

  def loadConfig(args: Array[String]): Unit = {
    //获取外部配置文件
    try {
      val configPath = args(0)
      val file = new FileInputStream(configPath)
      if (file != null) {
        val prop = new Properties()
        prop.load(file)
        println("读取外部配置文件信息....")
        println(prop)
        appName = prop.getProperty("appName", appName)
        sparkMaster = prop.getProperty("sparkMaster", sparkMaster)
        sparkOtherConfig_numThreads = prop.getProperty("sparkOtherConfig_numThreads", sparkOtherConfig_numThreads)
        hdfsUrl = prop.getProperty("hdfsUrl", hdfsUrl)
        hdfsDir = prop.getProperty("hdfsDir", hdfsDir)
        hdfsRatingsSourceDir = prop.getProperty("hdfsRatingsSourceDir", hdfsRatingsSourceDir)
        hdfsRatingsFileName = prop.getProperty("hdfsRatingsFileName", hdfsRatingsFileName)
        if (hdfsListenFileName.trim == "") {
          hdfsRatingsFileName = yesterdayYMD() + ".dat"
        }
        hdfsListenDir = prop.getProperty("hdfsListenDir", hdfsListenDir)
        hdfsListenFileName = prop.getProperty("hdfsListenFileName", hdfsListenFileName)
        if (hdfsListenFileName.trim == "") {
          hdfsListenFileName = yesterdayYMD() + ".dat"
        }

        hdfsSingDir = prop.getProperty("hdfsSingDir", hdfsSingDir)
        hdfsSingFileName = prop.getProperty("hdfsListenFileName", hdfsSingFileName)
        if (hdfsSingFileName.trim == "") {
          hdfsSingFileName = yesterdayYMD() + ".dat"
        }


        hbaseZookeeperPort = prop.getProperty("hbaseZookeeperPort", hbaseZookeeperPort)
        hbaseZookeeperQuorum = prop.getProperty("hbaseZookeeperQuorum", hbaseZookeeperQuorum)
        hbaseMaster = prop.getProperty("hbaseMaster", hbaseMaster)


        hbaseRecommendTableName = prop.getProperty("hbaseRecommendTableName", hbaseRecommendTableName)
        recommendCount = prop.getProperty("recommendCount", recommendCount.toString).toInt

        hbaseListenTableName = prop.getProperty("hbaseListenTableName", hbaseListenTableName)
        hbaseSingTableName = prop.getProperty("hbaseSingTableName", hbaseSingTableName)
      }
    } catch {
      case e: Exception => {
        println("read config error")
        e.printStackTrace()
        println("退出程序")
        System.exit(0)
      }
    }
  }


  def initTable(conf: Connection, tableName: TableName): Unit = {
    val hAdmin: HBaseAdmin = conf.getAdmin.asInstanceOf[HBaseAdmin]
    try {
      if (hAdmin.tableExists(tableName)) {
        hAdmin.disableTable(TableName)
        hAdmin.truncateTable(tableName, true)
        hAdmin.enableTable(TableName)
        println("清除表数据成功!")
      }
    } finally {
      hAdmin.close()
    }


  }


}
