package com.rec

import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.slf4j.LoggerFactory

/**
 * Created by Zealot on 16/1/4.
 */
object RecDemo {
  val log = LoggerFactory.getLogger(RecDemo.getClass);
  val conf = new SparkConf().setAppName(RecDemo.getClass.toString);
  val sc = new SparkContext(conf);
  val data = sc.textFile("/user/taoyizhou/recommender/rating/2016-01-03-all/").filter(_.split(",").length>=3).repartition(500)
  val ratings = data.map(_.split(',') match { case Array(user, item, rate) =>
    Rating(user.toInt, item.toInt, rate.toDouble)
  })
  def getModel(path: String): MatrixFactorizationModel ={
    MatrixFactorizationModel.load(sc, path)
  }

  /**
   * 获取所有用户id
   * @return
   */
  def getAllUsers():RDD[String]={
    data.map(_.split(",") match {case Array(user,item,rate)=>user})
  }

  /**
   * 从训练好的模型中，获取结果
   * @param modelPath
   */
  def getResult(modelPath:String): Unit ={
    val model = getModel(modelPath)
    val userRec = model.recommendProductsForUsers(5)
    userRec.foreach(_._1)

  }

  /**
   * 训练隐式数据
   * @param rank
   * @param numIterations
   * @param lambda
   * @param alpha
   * @return
   */
  def trainImplicit(rank:Int, numIterations:Int, lambda: Double, alpha: Double):String={


    log.info("Als Rec start....")

    val model = ALS.trainImplicit(ratings, rank, numIterations, lambda, alpha)

    // Evaluate the model on rating data
    val usersProducts = ratings.map { case Rating(user, product, rate) =>(user, product)}
    val predictions = model.predict(usersProducts).map { case Rating(user, product, rate) =>((user, product), rate)}
    val ratesAndPreds = ratings.map { case Rating(user, product, rate) =>((user, product), rate)}.join(predictions)


    /**
     * 使用自带的代码计算MSE和RMSE
     */
    val realAndPredictValue = ratesAndPreds.map(x=>(x._2._1,x._2._2))
    val regressionMetrics = new RegressionMetrics(realAndPredictValue)
    val MSE = regressionMetrics.meanSquaredError
    val RMSE = regressionMetrics.rootMeanSquaredError
    log.info("save Als Rec model....")
    // Save and load model
    model.save(sc, "/user/taoyizhou/recommender/AlsModel/"+System.currentTimeMillis()+"-MSE_"+MSE.toString+"-RMSE_"+RMSE.toString)
    "MSE: "+MSE.toString+", RMSE: "+RMSE.toString
  }

  def main(args: Array[String]) {
    val rank = 10
    val numIterations = 15
    val lambda = 0.02
    val alpha = 0.01
    val result = trainImplicit(rank, numIterations, lambda, alpha)
    log.info(s"ERR: $result")
  }
}
