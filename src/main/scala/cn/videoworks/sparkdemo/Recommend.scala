package cn.videoworks.sparkdemo

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel

object Recommend {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("recommend test")
      .setMaster("spark://10.2.17.177:7077")
      .set("spark.rpc.netty.dispatcher.numThreads", "2") //预防RpcTimeoutException


    val sc = new SparkContext(conf)

    //导入数据
    val base = "hdfs://10.2.17.178:8020/sparkdemo/0.8779186114278942"

    val model = MatrixFactorizationModel.load(sc, base)
    //给用户5推荐前评分前10的物品
    val recommendations = model.recommendProducts(319, 10)
    recommendations.map(x => {
      println(x.user + "-->" + x.product + "-->" + x.rating)
    })


  }

}
