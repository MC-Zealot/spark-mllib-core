///**
// * Created by Administrator on 2016/1/8.
// */
//
//import java.io.{File, PrintWriter}
//
//import breeze.numerics.sqrt
//import org.apache.spark.rdd.RDD
//import org.apache.spark.{SparkConf, SparkContext}
//
//object GBDTDEMO1 {
//  def main(args: Array[String]) {
//
//    val sparkConf = new SparkConf().setAppName("cf item-based").setMaster("local[10]")
//    val sc = new SparkContext(sparkConf)
//    //val hiveql = new HiveContext(sc)
//
//    //import hiveql.implicits._
//
//   //val oriRatings = sc.textFile("d:/DL85845_RESULT.txt").map(line => {
//   // val fields = line.split("\t")
//   //   (fields(0), fields(1), fields(2).toInt)
//   // })
//
//  // val temp = oriRatings.first()
//   //  print(temp)
//
//     val user_rdd =  UserData(sc,"d:/DL85845_RESULT.txt","\t")
//     val coor = Cooccurrence(user_rdd)
//
//    // var coor1 = Cooccurrence_Euclidean(user_rdd)
//
//   //  val result1 = Recommend(coor1,user_rdd,20)
//     val result = Recommend(coor,user_rdd,10)
//    // val result1 = result.foreach(println)
//
//     val writer = new PrintWriter(new File("d:/duowan.txt"))
//
//      for(line<-result.collect()){
//        writer.println(line)
//      }
//
//  //  val writer1 = new PrintWriter(new File("d:/duowan1.txt"))
//
//  //  for(line<-result1.collect()){
//  //    writer1.println(line)
//   // }
//  }
//
//
//  def UserData (
//
//    sc:SparkContext,input:String,
//
//    split:String
//
//  ):(RDD [(String,String,Double)]) = {
//
//    val user_rdd1= sc.textFile(input,10).filter(_.split("\t").length==3)
//
//    val user_rdd2=user_rdd1.map(line=> {
//
//      val fileds= line.split("\t")
//
//      (fileds(0),fileds(1),fileds(2).toDouble)
//
//    })
//
//    user_rdd2
//
//  }
//
//  def Cooccurrence (
//
//                     user_rdd:RDD[(String,String,Double)]
//
//                     ) : (RDD[(String,String,Double)]) = {
//
//    //  0 �����׼��
//
//    val user_rdd2=user_rdd.map(f => (f._1,f._2)).sortByKey()
//
//    user_rdd2.cache
//
//    //  1 (�û�����Ʒ)�ѿ���� (�û�����Ʒ) =>��Ʒ:��Ʒ���
//
//    val user_rdd3=user_rdd2 join user_rdd2
//
//    val user_rdd4=user_rdd3.map(data=> (data._2,1))
//
//    //  2 ��Ʒ:��Ʒ:Ƶ��
//
//    val user_rdd5=user_rdd4.reduceByKey((x,y) => x + y)
//
//    //  3 �ԽǾ���
//
//    val user_rdd6=user_rdd5.filter(f=> f._1._1 == f._1._2)
//
//    //  4 �ǶԽǾ���
//
//    val user_rdd7=user_rdd5.filter(f=> f._1._1 != f._1._2)
//
//    //  5 ����ͬ�����ƶȣ���Ʒ1����Ʒ2��ͬ��Ƶ�Σ�
//
//    val user_rdd8=user_rdd7.map(f=> (f._1._1, (f._1._1, f._1._2, f._2))).
//
//      join(user_rdd6.map(f=> (f._1._1, f._2)))
//
//    val user_rdd9=user_rdd8.map(f=> (f._2._1._2, (f._2._1._1,
//
//      f._2._1._2, f._2._1._3, f._2._2)))
//
//    val user_rdd10=user_rdd9.join(user_rdd6.map(f => (f._1._1, f._2)))
//
//    val user_rdd11 = user_rdd10.map(f => (f._2._1._1,f._2._1._2,f._2._1._3,f._2._1._4,f._2._2))
//
//    val user_rdd12=user_rdd11.map(f=> (f._1, f._2, (f._3 / sqrt(f._4 * f._5)) ))
//
//    //   6����
//
//    user_rdd12
//
//  }
//
//  def Cooccurrence_Euclidean (
//
//                     user_rdd:RDD[(String,String,Double)]
//
//                     ) : (RDD[(String,String,Double)]) = {
//
//    //  0 �����׼��
//
//    val user_rdd2=user_rdd.map(f => (f._1,f._2)).sortByKey()
//
//    user_rdd2.cache
//
//    //  1 (�û�����Ʒ)�ѿ���� (�û�����Ʒ) =>��Ʒ:��Ʒ���
//
//    val user_rdd3=user_rdd2 join user_rdd2
//
//    val user_rdd4=user_rdd3.map(data=> (data._2,1))
//
//    //  2 ��Ʒ:��Ʒ:Ƶ��
//
//    //val user_rdd5=user_rdd4.map(f=> (f._1,(f._2._1-f._2._2 )*(f._2._1-f._2._2 ))).reduceByKey(_+_)
//
//    val user_rdd5=user_rdd4.reduceByKey((x,y) => x + y)
//
//    //  3 �ԽǾ���
//
//    //val user_rdd5=user_rdd4.map(f=> (f._1,(f._2._1-f._2._2 )*(f._2._1-f._2._2 ))).reduceByKey(_+_)
//
//    //  3 ����Ʒ1,��Ʒ2,����1,����2����� => ����Ʒ1,��Ʒ2,1����ϲ��ۼ�   �����ص���
//
//    val user_rdd6=user_rdd4.map(f=> (f._1,1)).reduceByKey(_+_)
//
//    //  4 �ǶԽǾ���
//
//    val user_rdd7=user_rdd5.filter(f=> f._1._1 != f._1._2)
//
//    //  5 �������ƶ�
//
//    val user_rdd8=user_rdd7.join(user_rdd6)
//
//    val user_rdd9=user_rdd8.map(f=> (f._1._1,f._1._2,f._2._2/(1+sqrt(f._2._1))))
//
//    //   7 ����
//
//    user_rdd9
//
//  }
//
//  def CosineSimilarity (
//
//    user_rdd:RDD[(String,String,Double)]
//
//  ) : (RDD[(String,String,Double)]) = {
//
//    //  0 �����׼��
//
//    val user_rdd2=user_rdd.map(f => (f._1,(f._2,f._3))).sortByKey()
//
//    user_rdd2.cache
//
//    //  1 (�û�,��Ʒ,����)�ѿ���� (�û�,��Ʒ,����) =>����Ʒ1,��Ʒ2,����1,����2�����
//
//    val user_rdd3=user_rdd2 join user_rdd2
//
//    val user_rdd4=user_rdd3.map(f=> ((f._2._1._1, f._2._2._1),(f._2._1._2, f._2._2._2)))
//
//    //  2 ����Ʒ1,��Ʒ2,����1,����2����� => ����Ʒ1,��Ʒ2,����1*����2����ϲ��ۼ�
//
//    val user_rdd5=user_rdd4.map(f=> (f._1,f._2._1*f._2._2 )).reduceByKey(_+_)
//
//    //  3 �ԽǾ���
//
//      val user_rdd6=user_rdd5.filter(f=> f._1._1 == f._1._2)
//
//    //  4 �ǶԽǾ���
//
//    val user_rdd7=user_rdd5.filter(f=> f._1._1 != f._1._2)
//
//    //  5 �������ƶ�
//
//    val user_rdd8=user_rdd7.map(f=> (f._1._1, (f._1._1, f._1._2, f._2))).
//
//      join(user_rdd6.map(f=> (f._1._1, f._2)))
//
//    val user_rdd9=user_rdd8.map(f=> (f._2._1._2, (f._2._1._1,
//
//      f._2._1._2, f._2._1._3, f._2._2)))
//
//    val user_rdd10=user_rdd9.join(user_rdd6.map(f => (f._1._1, f._2)))
//
//    val user_rdd11 = user_rdd10.map(f => (f._2._1._1,f._2._1._2,f._2._1._3,f._2._1._4,f._2._2))
//
//    val user_rdd12=user_rdd11.map(f=> (f._1, f._2, (f._3 / sqrt(f._4 * f._5)) ))
//
//    //  7 ����
//
//    user_rdd12
//
//  }
//
//  def Recommend (
//
//    items_similar:RDD[(String,String,Double)],
//
//    user_perf:RDD[(String,String,Double)],
//
//    r_number:Int
//
//  ) : (RDD[(String,String,Double)]) = {
//
//    //  1 ������㡪��i����j��join
//
//    val rdd_app1_R2=items_similar.map(f => (f._2, (f._1,f._3))).
//
//      join(user_perf.map(f => (f._2,(f._1,f._3))))
//
//    //  2 ������㡪��i����j��Ԫ�����
//
//    val rdd_app1_R3=rdd_app1_R2.map(f=> ((f._2._2._1,f._2._1._1),f._2._2._2*f._2._1._2))
//
//    //  3 ������㡪���û���Ԫ���ۼ����
//
//    val rdd_app1_R4=rdd_app1_R3.reduceByKey((x,y)=> x+y).map(f => (f._1._1,(f._1._2,f._2)))
//
//    //  4 ������㡪���û����û��Խ�����򣬹���
//
//    val rdd_app1_R5=rdd_app1_R4.groupByKey()
//
//    val rdd_app1_R6=rdd_app1_R5.map(f=> {
//
//      val i2 = f._2.toBuffer
//
//      val i2_2 = i2.sortBy(_._2)
//
//      if (i2_2.length > r_number)i2_2.remove(0,(i2_2.length-r_number))
//
//      (f._1,i2_2.toIterable)
//
//    })
//
//    val rdd_app1_R7=rdd_app1_R6.flatMap(f=> {
//
//      val id2 = f._2
//
//      for (w <-id2)yield(f._1,w._1,w._2)
//
//    })
//
//    rdd_app1_R7
//
//  }
//  def EuclideanDistanceSimilarity (
//
//                                    user_rdd:RDD[(String,String,Double)]
//
//                                    ) : (RDD[(String,String,Double)]) = {
//
//    //  0 �����׼��
//
//    val user_rdd2=user_rdd.map(f => (f._1,(f._2,f._3))).sortByKey()
//
//    user_rdd2.cache
//
//    //  1 (�û�,��Ʒ,����)�ѿ���� (�û�,��Ʒ,����) =>����Ʒ1,��Ʒ2,����1,����2�����
//
//    val user_rdd3=user_rdd2 join user_rdd2
//
//    val user_rdd4=user_rdd3.map(f=> ((f._2._1._1, f._2._2._1),(f._2._1._2, f._2._2._2)))
//
//    //  2 ����Ʒ1,��Ʒ2,����1,����2����� => ����Ʒ1,��Ʒ2,����1-����2����ϲ��ۼ�
//
//    val user_rdd5=user_rdd4.map(f=> (f._1,(f._2._1-f._2._2 )*(f._2._1-f._2._2 ))).reduceByKey(_+_)
//
//    //  3 ����Ʒ1,��Ʒ2,����1,����2����� => ����Ʒ1,��Ʒ2,1����ϲ��ۼ�   �����ص���
//
//    val user_rdd6=user_rdd4.map(f=> (f._1,1)).reduceByKey(_+_)
//
//    //  4 �ǶԽǾ���
//
//    val user_rdd7=user_rdd5.filter(f=> f._1._1 != f._1._2)
//
//    //  5 �������ƶ�
//
//    val user_rdd8=user_rdd7.join(user_rdd6)
//
//    val user_rdd9=user_rdd8.map(f=> (f._1._1,f._1._2,f._2._2/(1+sqrt(f._2._1))))
//
//    //   7 ����
//
//    user_rdd9
//
//  }
//
//
//}
//
//
//
