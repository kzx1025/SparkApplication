package sparkApp

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.graphx.{PartitionStrategy, GraphLoader, GraphXUtils}

/**
  * Created by iceke on 16/5/12.
  */
object TriangleCount {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("PageRank").setMaster("local")
    GraphXUtils.registerKryoClasses(conf)
    val sc = new SparkContext(conf)
    val graph = GraphLoader.edgeListFile(sc, "data/followers.txt", true).partitionBy(PartitionStrategy.RandomVertexCut)
    // Find the triangle count for each vertex
    val triCounts = graph.triangleCount().vertices
    // Join the triangle counts with the usernames
    val users = sc.textFile("data/users.txt").map { line =>
      val fields = line.split(",")
      (fields(0).toLong, fields(1))
    }
    val triCountByUsername = users.join(triCounts).map { case (id, (username, tc)) =>
      (username, tc)
    }
    // Print the result
    println(triCountByUsername.collect().mkString("\n"))
  }

}
