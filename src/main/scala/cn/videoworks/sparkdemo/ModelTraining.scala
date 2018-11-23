package cn.videoworks.sparkdemo


import cn.videoworks.sparkdemo.conf.AppConfig
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.sql.{DataFrame, SparkSession}


object ModelTraining extends AppConfig {


  def main(args: Array[String]): Unit = {
    //设置log基本，生产也建议使用WARN
    Logger.getRootLogger.setLevel(Level.WARN)
    //    val iString = new SimpleDateFormat("yyyy_MM_dd").format(new Date())
    //    val path = s"hdfs://dtns/sparkdemo/model_2018_11_19";
    //    val loadModel = ALSModel.load(path)
    //    val testRecommend = loadModel.recommendForAllUsers(2)
    //    testRecommend.show(20, false)

    //获取外部配置文件
    loadConfig(args)

    //    创建sparkSession
    val sparkConf = new SparkConf().setAppName(appName).setMaster(sparkMaster).set("spark.rpc.netty.dispatcher.numThreads", sparkOtherConfig_numThreads)
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    //导入数据
    val fileRegex = "/part-*"
    val ratingsData = spark.read.textFile(hdfsUrl + hdfsRatingsSourceDir + hdfsRatingsFileName + fileRegex).rdd
    val parts = ratingsData.map(line => parseRating(line))
    val allRatingsData = spark.createDataFrame(parts).cache()

    //切分数据 分别用于训练 (60%), 校验 (20%), and 测试 (20%)
    //目前先随机拆分
    //可按照日期拆分
    val Array(training, validation, test) = allRatingsData.randomSplit(Array(0.6, 0.2, 0.2), 12)
    val tr = training.cache()
    val v = validation.cache()
    val te = test.cache()
    println("验证样本基本信息为：")
    println("训练样本数：" + tr.count())
    println("验证样本数：" + v.count())
    println("测试样本数：" + te.count())

    val als = new ALS()
      .setMaxIter(10)
      .setRank(10)
      .setRegParam(0.01)
      .setNonnegative(true)
      .setUserCol("userId")
      .setItemCol("sourceId")
      .setRatingCol("rating")
    val model = als.fit(tr)
    val predictionsImplicit = model.transform(te)

    println("开始打印样例...20例显示")
    predictionsImplicit.show(20)


    if (predictionsImplicit.count() == 0) {
      println("无测试数据")

    } else {
      val predictions1 = predictionsImplicit.na.drop()
      val evaluator = new RegressionEvaluator()
        .setMetricName("rmse")
        .setLabelCol("rating")
        .setPredictionCol("prediction")
      val rmse = evaluator.evaluate(predictions1)

      println(s"Mean Squared Error -------> $rmse")
    }
    // Save and load model
    val path = hdfsUrl + hdfsDir + yesterdayYMD()
    println(s"save.... path$path")
    model.write.overwrite().save(path)

    spark.stop()
    //

  }


}

