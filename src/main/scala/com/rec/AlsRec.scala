package com.rec

import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.recommendation.{ ALS, MatrixFactorizationModel, Rating}
import org.slf4j.{ LoggerFactory}

/**
 * Created by Zealot on 15/12/21.
 */
object AlsRec {
  val log = LoggerFactory.getLogger(AlsRec.getClass);
  val conf = new SparkConf().setAppName("Collaborative Filtering Example").setMaster("local[2]");
  val sc = new SparkContext(conf);
  val data = sc.textFile("file:///Users/Zealot/Downloads/ml-1m/test.dat").filter(_.split(",").length>=3)
  val ratings = data.map(_.split(',') match { case Array(user, item, rate) =>
    Rating(user.toInt, item.toInt, rate.toDouble)
  })
  def train():String={

//    sc.textFile("/user/taoyizhou/recommender/rating/artist/2016-03-01-all-explict/").filter(_.split(":").length>1).flatMap(x=> {
//      val ss = x.split(":")
//      val uniqueaIds = ss(1)
//      val aid = ss(0)
//      uniqueaIds.split(",")
//    }).count




    log.info("Als Rec start....")

    // Load and parse the data
    val data = sc.textFile("file:///Users/Zealot/Downloads/ml-1m/test.dat").repartition(500)
    val ratings = data.map(_.split(',') match { case Array(user, item, rate) =>
      Rating(user.toInt, item.toInt, rate.toDouble)
    })
    val splits = ratings.randomSplit(Array(0.6, 0.4), seed = 11L)
    val trainingData = splits(0)

    val testData = splits(1)
    // Build the recommendation model using ALS
    val rank = 1
    val numIterations = 12
    val lambda = 0.02
    val alpha = 0.01
    val model = ALS.train(ratings, rank, numIterations, lambda)
    //    val model = ALS.trainImplicit(ratings, rank, numIterations, lambda, alpha);

    // Evaluate the model on rating data
    val usersProducts = ratings.map { case Rating(user, product, rate) =>(user, product)};
    val predictions = model.predict(usersProducts).map { case Rating(user, product, rate) =>((user, product), rate)};
    val ratesAndPreds = ratings.map { case Rating(user, product, rate) =>((user, product), rate)}.join(predictions)




    val MSE = ratesAndPreds.map { case ((user, product), (r1, r2)) =>
      val err = (r1 - r2)
      err * err
    }.mean()
//    println("Mean Squared Error = " + MSE)

    val RMSE = math.sqrt(MSE)
    // Save and load model
    model.save(sc, "file:///Users/Zealot/Downloads/als/model_"+System.currentTimeMillis())
    "MSE: "+MSE.toString+", RMSE: "+RMSE.toString
  }

  def trainImplicit(rank:Int, numIterations:Int, lambda: Double, alpha: Double):String={


    log.info("Als Rec start....")

    // Load and parse the data
    val data = sc.textFile("file:///Users/Zealot/Downloads/ml-1m/test.dat")
    val ratings = data.map(_.split(',') match { case Array(user, item, rate) =>
      Rating(user.toInt, item.toInt, rate.toDouble)
    })
    // Build the recommendation model using ALS
//    val rank = 1
//    val numIterations = 12
//    val lambda = 0.02
//    val alpha = 0.01
    val model = ALS.train(ratings, rank, numIterations, lambda)
    //    val model = ALS.trainImplicit(ratings, rank, numIterations, lambda, alpha);

    // Evaluate the model on rating data
    val usersProducts = ratings.map { case Rating(user, product, rate) =>(user, product)};
    val predictions = model.predict(usersProducts).map { case Rating(user, product, rate) =>((user, product), rate)};
    val ratesAndPreds = ratings.map { case Rating(user, product, rate) =>((user, product), rate)}.join(predictions)


    /**
     * 使用自带的代码计算MSE和RMSE
     */
    val realAndPredictValue = ratesAndPreds.map(x=>(x._2._1,x._2._2))
    val regressionMetrics = new RegressionMetrics(realAndPredictValue)
    val MSE = regressionMetrics.meanSquaredError
    val RMSE = regressionMetrics.rootMeanSquaredError

    // Save and load model
    model.save(sc, "file:///Users/Zealot/Downloads/als/model_"+System.currentTimeMillis())
    "MSE: "+MSE.toString+", RMSE: "+RMSE.toString
  }

  def getModel(path: String): MatrixFactorizationModel ={
    MatrixFactorizationModel.load(sc, path)
  }

  def getAllUsers():RDD[String]={
   data.map(_.split(",") match {case Array(user,item,rate)=>user})
  }

  def getResult(modelPath:String): Unit ={
    val model = getModel(modelPath)
        val userRec = model.recommendProductsForUsers(5)
    userRec.map(x=>{
      val array = x._2
      val ss = new StringBuffer()
      array.foreach(x=>(ss.append(x.user+","+x.product+","+x.rating).append("\t")))
      ss.toString
      }
    ).foreach(println)

//        userRec.foreach(_._1)
//    model.recommendProducts(38224176,20).take(20).foreach(println)
    val users = ratings.map(_.user).distinct()
    users.collect.flatMap { user =>
      model.recommendProducts(user, 10)
    }.foreach(println)
    users.foreach{
      user=>{
        val rs: Array[Rating] = model.recommendProducts(user.toInt,20)
        var value=""
        var key = 0;
        rs.foreach{
          r=>{
            key = r.user
            value = value + r.product + ":" + r.rating + ","
          }
          print(key.toString+ "\t" +value)
        }
      }
    }

  }

  def main(args: Array[String]): Unit = {
    val MSE = train
    log.info("Mean Squared Error = " + MSE)
//      val result = getResult("/Users/Zealot/Downloads/als/model_1450682349509")
    val rank = 1
    val numIterations = 12
    val lambda = 0.02
    val alpha = 0.01
    val result = trainImplicit(rank, numIterations, lambda, alpha)


//    val result = 1;
    log.info(s"ERR: $result")


  }


}
