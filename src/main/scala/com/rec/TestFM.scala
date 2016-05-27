package com.rec


import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.{SparkConf, SparkContext}


/**
 * Created by zrf on 4/18/15.
 */


object TestFM extends App {

  override def main(args: Array[String]): Unit = {

    val sc = new SparkContext(new SparkConf().setAppName("TESTFM"))
//
//    //    "hdfs://ns1/whale-tmp/url_combined"
//    val training = MLUtils.loadLibSVMFile(sc, "hdfs://ns1/whale-tmp/url_combined").cache()
//
//    //    val task = args(1).toInt
//    //    val numIterations = args(2).toInt
//    //    val stepSize = args(3).toDouble
//    //    val miniBatchFraction = args(4).toDouble
//
//    val fm1 = FMWithSGD.train(training, task = 1, numIterations = 100, stepSize = 0.15, miniBatchFraction = 1.0, dim = (true, true, 4), regParam = (0, 0, 0), initStd = 0.1)
//
//
//    val fm2 = FMWithLBFGS.train(training, task = 1, numIterations = 20, numCorrections = 5, dim = (true, true, 4), regParam = (0, 0, 0), initStd = 0.1)

    val resultFile = "hdfs://hcat6cluster/spark-recommend/resultrem_"+getNowDate


  }
  def getNowDate():String={
    var now: Date = new Date()
    var  dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    var nowDate = dateFormat.format( now )
    nowDate
  }
  def isNumeric(str: String):Boolean={
    if(str==null){
      return false
    }
    var i = 0;
    while(i < str.length){
      if (str.charAt(i)!='.'  && !Character.isDigit(str.charAt(i))) {
        return false;
      }
      i+=1
    }

    true
  }
  def getYesterday():String= {
    var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    var cal: Calendar = Calendar.getInstance()
    cal.add(Calendar.DATE, -1)
    var yesterday = dateFormat.format(cal.getTime())
    yesterday
  }
  def getNdayBefore(n:Integer):String= {
    var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    var cal: Calendar = Calendar.getInstance()
    cal.add(Calendar.DATE, -n)
    var yesterday = dateFormat.format(cal.getTime())
    yesterday
  }
}
