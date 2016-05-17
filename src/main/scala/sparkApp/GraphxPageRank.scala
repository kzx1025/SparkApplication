package sparkApp

import org.apache.spark.graphx.{GraphXUtils, GraphLoader}
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by iceke on 16/5/12.
  */
object GraphxPageRank {
  def main(args: Array[String]){
    val conf = new SparkConf().setAppName("PageRank").setMaster("local")
     GraphXUtils.registerKryoClasses(conf)
    val sc = new SparkContext(conf)
    val graph = GraphLoader.edgeListFile(sc, "data/followers.txt")
    // Run PageRank
    val ranks = graph.pageRank(0.00001).vertices
    // Join the ranks with the usernames
    val users = sc.textFile("data/users.txt").map { line =>
      val fields = line.split(",")
      (fields(0).toLong, fields(1))
    }
    val ranksByUsername = users.join(ranks).map {
      case (id, (username, rank)) => (username, rank)
    }
    // Print the result
    println(ranksByUsername.collect().mkString("\n"))
  }

}
