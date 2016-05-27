package com.graph
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by Administrator on 2016/4/1.
 */
object SparkGraphx1  {

  def main(args: Array[String]) {
    System.out.println("============"+java.lang.Long.parseLong("6"))
    val conf = new SparkConf().setAppName("graphxdemo1").setMaster("local[2]")
    val sc = new SparkContext(conf)
    //    val vertexFile = " hdfs://hdfscluster/tmp/lijunyi/input/graph1_vertex.csv"
    //   val edgeFile = " hdfs://hdfscluster/tmp/lijunyi/input/graph1_edges.csv"

    val vertexFile = "file:///Users/Zealot/Downloads/graph1_vertexs.txt"
    val edgeFile = "file:///Users/Zealot/Downloads/graph3_edges.txt"
    val vertices: RDD[(VertexId,(String,String))] =
      sc.textFile(vertexFile).map{line =>

        val fields = line.split(",")
        (java.lang.Long.parseLong(fields(0).trim), (fields(1),fields(2)))

      }

    val edges : RDD[Edge[String]] =

      sc.textFile(edgeFile).map { line =>
        val fields = line.split(",")
        Edge( java.lang.Long.parseLong(fields(0).trim), fields(1).toLong,fields(2))
      }


    val default = ("joeylee","Missing")
    val graph = Graph(vertices,edges,default)

    print("vertices " + graph.vertices.count)
    print("edges" + graph.edges.count)


  }
}