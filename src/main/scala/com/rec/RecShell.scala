package com.rec

import java.util

import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.slf4j.LoggerFactory

/**
 * Created by Zealot on 16/1/5.
 */
object RecShell {
  /**
  val rank = 5
  val numIterations = 15
  val lambda = 0.02
  val alpha = 0.01
  RMSE: Double = 6.791411450782698
  MSE: 46.123269493822356

  val rank = 10
  val numIterations = 20
  val lambda = 0.005
  val alpha = 0.01
  RMSE: Double = 6.7421709303590465
  MSE: 45.45686885417857



  val rank = 10
  val numIterations = 20
  val lambda = 0.01     *
  val alpha = 0.01
  RMSE: Double = 6.741343111345678
  MSE: Double = 45.445706944887824


  val rank = 10
  val numIterations = 20
  val lambda = 0.02
  val alpha = 0.01
  RMSE: Double = 6.744836556350006
  MSE: 45.492820171875415

  val rank = 10
  val numIterations = 20
  val lambda = 0.05
  val alpha = 0.01
  RMSE: Double = 6.77597874661681
  MSE: 45.91388797460272

  val rank = 10
  val numIterations = 20
  val lambda = 0.01
  val alpha = 0.1
  MSE: Double = 80.14662677689387
  RMSE: Double = 8.952464843655845

      ======================================================

  good

  val rank = 15
  val numIterations = 20
  val lambda = 0.01
  val alpha = 0.01
  MSE: Double = 44.97945410233915
  RMSE: Double = 6.70667235686515
     ====================================================

   */
  val conf = new SparkConf().setAppName(RecDemo.getClass.toString);
  val sc = new SparkContext(conf);
//  val data = sc.textFile("/user/taoyizhou/recommender/rating/2016-01-03-all/").filter(_.split(",").length>=3).repartition(500)
//  val model = ALS.trainImplicit(ratings, rank, numIterations, lambda, alpha)
  val data = sc.textFile("/user/taoyizhou/recommender/rating/2016-05-11-all-explict/").filter(_.split(",").length>=3)
  val ratings = data.map(_.split(',') match { case Array(user, item, rate) =>
    Rating(user.toInt, item.toInt, rate.toDouble)
  })
  val rank = 50
  val numIterations = 20
  val lambda = 0.01
  val alpha = 0.01

  val model = ALS.train(ratings,rank,numIterations,lambda)

  val usersProducts = ratings.map { case Rating(user, product, rate) =>(user, product)}
  val predictions = model.predict(usersProducts).map { case Rating(user, product, rate) =>((user, product), rate)}
  val ratesAndPreds = ratings.map { case Rating(user, product, rate) =>((user, product), rate)}.join(predictions)
  val realAndPredictValue = ratesAndPreds.map(x=>(x._2._1,x._2._2))
  val regressionMetrics = new RegressionMetrics(realAndPredictValue)
  val MSE = regressionMetrics.meanSquaredError
  val RMSE = regressionMetrics.rootMeanSquaredError
  println("MSE: "+MSE)
  println("RMSE:"+RMSE)

  var map = new util.HashMap[Int,String]

  for(rank <- 40 to 50){
    trainAls(rank)
  }


  def trainAls(num:Int): Unit ={
//    var list = new util.ArrayList()
    val rank = num
    val numIterations = 20
    val lambda = 0.01
    val alpha = 0.01

    val model = ALS.train(ratings,rank,numIterations,lambda)

    val usersProducts = ratings.map { case Rating(user, product, rate) =>(user, product)}
    val predictions = model.predict(usersProducts).map { case Rating(user, product, rate) =>((user, product), rate)}
    val ratesAndPreds = ratings.map { case Rating(user, product, rate) =>((user, product), rate)}.join(predictions)
    val realAndPredictValue = ratesAndPreds.map(x=>(x._2._1,x._2._2))
    val regressionMetrics = new RegressionMetrics(realAndPredictValue)
    val MSE = regressionMetrics.meanSquaredError
    val RMSE = regressionMetrics.rootMeanSquaredError
    val list  = List(num,MSE,RMSE)
    map.put(num,list.toString())
  }

}

/**
val rank = 15
  val numIterations = 20
  val lambda = 0.01
  val alpha = 0.01
  MSE:  Double = 1.238231588760868
  RMSE: Double = 1.1127585491744685


  val rank = 20
  val numIterations = 20
  val lambda = 0.01
  val alpha = 0.01
  MSE: Double = 0.9047475952908166
  RMSE: Double = 0.9511822093010448

  val rank = 20
  val numIterations = 20
  val lambda = 0.02
  val alpha = 0.01
  MSE: Double = 0.9233597924672209
  RMSE: Double = 0.9609161214524506

  val rank = 21
  val numIterations = 20
  val lambda = 0.01
  val alpha = 0.01
  MSE: Double = 1.6269758287601879
  RMSE: Double = 1.2755296267669316

  val rank = 25
  val numIterations = 20
  val lambda = 0.01
  val alpha = 0.01
  MSE: Double = 1.342907770434089
  RMSE: Double = 1.1588389751963337

  val rank = 30
  val numIterations = 20
  val lambda = 0.01
  val alpha = 0.01
  MSE: Double = 1.0854038800968064
  RMSE: Double = 1.0418271834123

  val rank = 40
  val numIterations = 20
  val lambda = 0.01
  val alpha = 0.01
  MSE: Double = 0.7617457844747961
  RMSE: Double = 0.8727804904297507

  val rank = 50
  val numIterations = 20
  val lambda = 0.01
  val alpha = 0.01
  MSE: Double = 0.5550363249873052
  RMSE: Double = 0.7450076006238494
  **/