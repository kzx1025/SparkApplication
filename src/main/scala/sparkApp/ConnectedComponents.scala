package sparkApp

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.graphx.{GraphLoader, GraphXUtils}

/**
  * Created by iceke on 16/5/12.
  */
object ConnectedComponents {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("PageRank").setMaster("local")
    GraphXUtils.registerKryoClasses(conf)
    val sc = new SparkContext(conf)
    // Load the graph as in the PageRank example
    val graph = GraphLoader.edgeListFile(sc, "data/followers.txt")
    // Find the connected components
    val cc = graph.connectedComponents().vertices
    // Join the connected components with the usernames
    val users = sc.textFile("data/users.txt").map { line =>
      val fields = line.split(",")
      (fields(0).toLong, fields(1))
    }
    val ccByUsername = users.join(cc).map {
      case (id, (username, cc)) => (username, cc)
    }
    // Print the result
    println(ccByUsername.collect().mkString("\n"))
  }

}
