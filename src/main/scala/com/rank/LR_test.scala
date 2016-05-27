package com.rank

import com.rec.RecDemo
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.classification.{LogisticRegressionWithLBFGS, LogisticRegressionModel}

import org.apache.spark.mllib.regression.LabeledPoint

import org.apache.spark.mllib.linalg.{Vector, Vectors}
/**
 * Created by Zealot on 16/1/10.
 */
class LR_test {
  val conf = new SparkConf().setAppName(RecDemo.getClass.toString);
  val sc = new SparkContext(conf);
  val data = sc.textFile("file:///Users/Zealot/Downloads/data2.txt").filter(_.split(",").length==32)
  val parsedData = data.map{line =>
    val parts = line.split(",")
    LabeledPoint(parts(31).toDouble, Vectors.dense(parts.slice(1,31).map(x =>x.toDouble)))
  }
//  parsedData.foreach(println)
  val splits = parsedData.randomSplit(Array(0.6, 0.4), seed = 11L)

  val trainingData = splits(0)

  val testData = splits(1)
  val model = new LogisticRegressionWithLBFGS().run(trainingData)

  val labelAndPreds = testData.map { point =>
    val prediction = model.predict(point.features)
    (point.label, prediction)
  }
  model.weights
  val trainErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / testData.count


  data.map { line =>

    var parts = line.split(",")
    parts.zipWithIndex.toMap
  }
//  .reduceByKey(_+_)
  model.clearThreshold()
model.predict(Vectors.dense(Array(17.4,22.3,27.1,27.0,27.0,27.1,27.3,27.4,27.5,27.6,27.8,27.8,27.8,27.8,27.8,27.8,27.8,27.8,27.9,28.0,28.1,28.2,28.2,28.2,28.1,28.0,27.9,27.8,27.7,27.6)))
  val sv2:Vector=Vectors.sparse(2,Seq((1,1.0),(2,2.0)))

}
