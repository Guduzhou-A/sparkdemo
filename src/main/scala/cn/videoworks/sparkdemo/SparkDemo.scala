package cn.videoworks.sparkdemo

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.util.Random


object SparkDemo {

  case class Rating(userId: Int, movieId: Int, rating: Float)

  def parseRating(str: String): Rating = {
    val fields = str.split("::")
    assert(fields.size == 4)
    Rating(fields(0).toInt, fields(1).toInt, fields(2).toFloat)
  }


  def main(args: Array[String]): Unit = {
    //设置log基本，生产也建议使用WARN
    Logger.getRootLogger.setLevel(Level.WARN)
    //    System.setProperty("hadoop.home.dir", "D:\\hadoop\\hadoop-2.9.0")
    //spark config
    val conf = new SparkConf()
      .setAppName("spark demo")
      .setMaster("spark://10.2.17.177:7077")
      .set("spark.rpc.netty.dispatcher.numThreads", "2") //预防RpcTimeoutException
    //设置环境变量
    val sc = new SparkContext(conf)

    val spark = SparkSession.builder()
      .config(conf).getOrCreate()

    //导入数据
    val base = "hdfs://10.2.17.178:8020/sparkdemo/"
    val moviesData = spark.read.textFile(base + "ratings.dat").rdd
    val parts = moviesData.map(line => parseRating(line))
    val allMovies = spark.createDataFrame(parts).cache()

    //切分数据 分别用于训练 (60%), 校验 (20%), and 测试 (20%)
    //目前先随机拆分
    //可按照日期拆分
    val Array(training, validation, test) = allMovies.randomSplit(Array(0.6, 0.2, 0.2), 12)

    println("验证样本基本信息为：")
    println("训练样本数：" + training.count())
    println("验证样本数：" + validation.count())
    println("测试样本数：" + test.count())

    val als = new ALS()
      .setMaxIter(10)
      .setRank(10)
      .setRegParam(0.01)
      .setNonnegative(true)
      .setUserCol("userId")
      .setItemCol("movieId")
      .setRatingCol("rating")
    val model = als.fit(training)
    val predictionsImplicit = model.transform(test)

    //    val pipeline = new Pipeline().setStages(Array(als))
    //    val model = pipeline.fit(training)
    //    val predictions = model.transform(test)

    println("开始打印样例...20例显示")
    predictionsImplicit.show(20)


    val predictions1 = predictionsImplicit.na.drop()

    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("rating")
      .setPredictionCol("prediction")
    val rmse = evaluator.evaluate(predictions1)

    println(s"Mean Squared Error -------> $rmse")


    // Save and load model
    val iString = new SimpleDateFormat("yyyy_MM_dd").format(new Date())
    val path = s"hdfs://10.2.17.178:8020/sparkdemo/model_$iString";

    println(s"save.... path$path")
    model.write.overwrite().save(path)

    println("开始为所有用户推荐3部电影 只显示 20条展示")
    val userRecs = model.recommendForAllUsers(3)
    val df = userRecs.withColumn("recommendations", userRecs("recommendations").getField("movieId").cast(StringType)).withColumnRenamed("recommendations", "movieIds")
    df.show(20, false)
    //    val conf = HBaseConfiguration.create()  
    //        //设置zooKeeper集群地址，也可以通过将hbase-site.xml导入classpath，但是建议在程序里这样设置  
    //        conf.set("hbase.zookeeper.quorum","slave1,slave2,slave3")  
    //        //设置zookeeper连接端口，默认2181  
    //        conf.set("hbase.zookeeper.property.clientPort", "2181")  
    //        conf.set(TableInputFormat.INPUT_TABLE, tablename)  

    spark.stop()
    //

  }


}

