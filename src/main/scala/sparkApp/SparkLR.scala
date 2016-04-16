package sparkApp

import java.util.Random

import breeze.linalg.Vector
import breeze.linalg.DenseVector
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by root on 15-9-7.
 */
object SparkLR {

  val rand = new Random(42)

  case class DataPoint(x: Vector[Double], y: Double)

  def main(args: Array[String]) {

    if(args.length<4){
      System.err.println("Usage of Parameters: input numpartition iter numDest name")
      System.exit(1)
    }

    val sparkConf = new SparkConf().setAppName(args(4))
    val sc = new SparkContext(sparkConf)
    val length = args(3).toInt
    val numPartitions = args(1).toInt
    /**val points = sc.textFile(args(0)).repartition(numPartitions).map{line =>
      val new_line = line.substring(line.lastIndexOf("(")+1,line.indexOf(")"))
      val parts = new_line.split(", ")
      println(parts.length)
      val litera = line.substring(line.lastIndexOf(",")+1,line.lastIndexOf(")"))
      val y = litera.toDouble
      val data = new Array[Double](parts.length)
      for(i <- 0 until parts.length){
        data(i) = parts(i).toDouble
      }
      val x = DenseVector(data)
      DataPoint(x,y)
    }.cache()**/
    val points = sc.objectFile(args(0)).asInstanceOf[RDD[SparkLR.DataPoint]].persist(StorageLevel.MEMORY_AND_DISK)

    points.foreach(x => Unit)
    val iterations = args(2).toInt

    // Initialize w to a random value
    var w = DenseVector.fill(length){2 * rand.nextDouble - 1}
    println("Initial w: " + w)

    for (i <- 1 to iterations) {
      println("On iteration " + i)
      val gradient = points.map { p =>
        p.x * (1 / (1 + Math.exp(-p.y * (w.dot(p.x)))) - 1) * p.y
      }.reduce(_ + _)
      w -= gradient
    }

    println("Final w: " + w)
    System.gc()
    System.gc()
    System.gc()

    sc.stop()
  }
}
