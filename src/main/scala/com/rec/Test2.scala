package com.rec


import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by Administrator on 2015/12/28.
 *
 * Modified by June on 2016/02/28.
 */

import org.apache.spark.mllib.evaluation.RegressionMetrics




object Test2 {

  val conf = new SparkConf().setAppName("Duowanbox_model")

  val sc = new SparkContext(conf)


  val resultFile = "file:///Users/Zealot/Downloads/DL106601_RESULT.csv"


  val K = 2.0
  // # folds of cross validation
  val num =10 // # recommendations per user


  def load(dataFile: String): RDD[StrIDRating] = {

    val rawData3 = sc.textFile(dataFile)

    var temp3 = rawData3.first()
    val rawRatings3 = rawData3.map(_.replace("\\N","0").split("\\|").take(6))

    rawRatings3.map { case Array(ts, uid, aid, uts, vts, score) => StrIDRating(uid, aid, score.toDouble) }.cache()
  }

  def predict(model: MatrixFactorizationModel, revUIDToInt: RDD[(Int, String)], revPIDToInt: RDD[(Int, String)], result: String): Unit = {

    var results: List[Rating] = Nil
    val recommendations = model.recommendProductsForUsers(num).collect().foldLeft(results)((a, b) => a ::: b._2.toList)

    println("#recommendations: " + recommendations.size)

    val expRecs: RDD[(String, String, Double)] = sc.parallelize(recommendations).keyBy(_.user).join(revUIDToInt).keyBy(_._2._1.product).join(revPIDToInt).map(r =>
      (r._2._1._2._2,
        r._2._2,
        r._2._1._2._1.rating)) //value: Double


    //   expRecs.saveAsTextFile(result)



    //    Printer.printToFile(new File(result))(p =>
    //      expRecs.collect.sortBy(_._1).foreach(r => p.println(r._1 + "\t" + r._2 + "\t" + r._3))
    //    )
  }


  def train(data: RDD[Rating]): MatrixFactorizationModel = {

    val start1 = System.currentTimeMillis()

    val end1 = System.currentTimeMillis()

    val start = System.currentTimeMillis()
    val model = ALS.train(data, 20, 10, 0.01)
    //   val model = ALS.trainImplicit(data,50,10,)
    val end = System.currentTimeMillis()
    println("Train Time = " + (end - start) * 1.0 / 1000)

    model
  }

  def test(model: MatrixFactorizationModel, testing: RDD[Rating]): Unit = {
    val uidsAids = testing.map { case Rating(uid, aid, score) => (uid, aid) }
    val predictions = model.predict(uidsAids).map { case Rating(aid, uid, score) => ((aid, uid), score) }
    val ratingsAndPredictions = testing.map { case Rating(uid, aid, score) => ((uid, aid), score) }.join(predictions)
    val predictedTrue = ratingsAndPredictions.map { case ((user, product), (predicted, actual)) => (predicted, actual) }

    val regressionMetrics = new RegressionMetrics(predictedTrue)
    println("Mean Square error = " + regressionMetrics.meanSquaredError)
    println("root Mean Square error=" + regressionMetrics.rootMeanSquaredError)
    println("_____________")
  }
  def nnHash(tag: String) = tag.hashCode & 0x7FFFFFFF
  def main(args: Array[String]) {

    val dataFile = "file:///Users/Zealot/Downloads/datafile1.txt"
    val resultFile = "file:///Users/Zealot/Downloads/result_0518"
    val sIDRatings = load(dataFile)

    //记录uid字符串与整数的映射,放到内存中
    val uidHashes = sIDRatings.map(r => r.uID).distinct.map(uid => (nnHash(uid), uid))
    //记录mid字符串与整数的映射
    val midHashes = sIDRatings.map(r => r.mID).distinct.map(mid => (nnHash(mid), mid))
    val ratings3 = sIDRatings.map { r => Rating(nnHash(r.uID), nnHash(r.mID), (r.value)) }

    val weights = Array(0.7, 0.3) //Array.fill(K.toInt)(1.0 / K)
    val partitions = ratings3.randomSplit(weights)

    val training = partitions(0).cache()
    val testing = partitions(1).cache()

    val model = train(training)

    val uidTable = sc.broadcast(uidHashes.collectAsMap())
    val midTable = sc.broadcast(midHashes.collectAsMap)

    val recommendations1 = model.recommendProductsForUsers(num).map(x => {
      val uid = x._1
      val array: Array[org.apache.spark.mllib.recommendation.Rating] = x._2

      var sb = new StringBuffer()
      array.foreach(x => {
        val uid = x.user
        val product = x.product
        val rating = x.rating
        sb.append(midTable.value.get(product) + ":" + rating).append("\t")
      })
      (uidTable.value.get(uid), sb.toString)
    })

    recommendations1.saveAsTextFile(resultFile)

  }


}

