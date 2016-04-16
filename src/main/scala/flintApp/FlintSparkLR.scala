package flintApp

/**
 * Created by root on 15-9-25.
 */

import org.apache.spark._

import java.util.Random

import org.apache.spark.rdd.RDD
import sparkApp.SparkLR

import scala.math.exp

import breeze.linalg.{Vector, DenseVector}

import java.io.{DataOutputStream, ByteArrayOutputStream}

import org.apache.hadoop.io.WritableComparator

class PointChunk(dimensions: Int,size: Int = 4196) extends ByteArrayOutputStream(size) { self =>

  def getVectorValueIterator(w: Array[Double]) = new Iterator[Array[Double]] {
    var offset = 0
    var currentPoint=new Array[Double](dimensions)
    var i = 0
    var y = 0.0
    var dotvalue = 0.0

    override def hasNext = offset < self.count

    override def next() = {
      if (!hasNext) Iterator.empty.next()
      else {
        //read data from the chunk
        i=0
        while (i < dimensions) {
          currentPoint(i)= WritableComparator.readDouble(buf, offset)
          offset += 8
          i += 1
        }
        y = WritableComparator.readDouble(buf, offset)
        offset += 8
        //calculate the dot value
        i=0
        dotvalue = 0.0
        while (i < dimensions) {
          dotvalue += w(i)*currentPoint(i)
          i += 1
        }
        //transform to values
        i=0
        while (i < dimensions) {
          currentPoint(i) *= (1 / (1 + exp(-y * dotvalue)) - 1) * y
          i += 1
        }
        currentPoint.clone()
      }
    }
  }
}
/**
 * Logistic regression based classification.
 * Usage: SparkLR [slices]
 *
 * This is an example implementation for learning how to use Spark. For more conventional use,
 * please refer to either org.apache.spark.mllib.classification.LogisticRegressionWithSGD or
 * org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS based on your needs.
 */

object FlintSparkLR {
  val rand = new Random(42)


  def testOptimized(points: RDD[SparkLR.DataPoint],iterations:Int,numDests:Int,w:DenseVector[Double]): Unit = {
    val cachedPoints = points.mapPartitions ({ iter =>
      val (iterOne ,iterTwo) = iter.duplicate
      val chunk = new PointChunk(numDests,8*iterOne.length*(1+numDests))
      val dos = new DataOutputStream(chunk)
      for (point <- iterTwo) {
        point.x.foreach(dos.writeDouble)
        dos.writeDouble(point.y)
      }
      Iterator(chunk)
    },true).cache()

    cachedPoints.foreach(x => Unit)

    val w_op=new Array[Double](numDests)
    for(i <- 0 until numDests)
      w_op(i) = w(i)

    val startTime = System.currentTimeMillis
    for (i <- 1 to iterations) {
      println("On iteration " + i)
      val gradient= cachedPoints.mapPartitions{ iter =>
        val chunk = iter.next()
        chunk.getVectorValueIterator(w_op)
      }.reduce{(lArray, rArray) =>
        val result_array=new Array[Double](lArray.length)
        for(i <- 0 to numDests-1)
          result_array(i) = lArray(i) + rArray(i)
        result_array
      }

      for(i <- 0 to numDests-1)
        w_op(i) = w_op(i) - gradient(i)
    }
    val duration = System.currentTimeMillis - startTime
    println("result:"+w_op.mkString(";"))
    println("Duration is " + duration / 1000.0 + " seconds")

  }

  def main(args: Array[String]) {

    if(args.length<4){
      System.err.println("Usage of Parameters: input numpartition iter numDest name")
      System.exit(1)
    }

    val sparkConf = new SparkConf().setAppName(args(4))
    val sc = new SparkContext(sparkConf)
    val numSlices = args(1).toInt
    val iterations = args(2).toInt
    val numDests = args(3).toInt
   /** val points = sc.textFile(args(0)).repartition(numSlices).map(line => {
      val new_line = line.substring(line.lastIndexOf("(")+1,line.indexOf(")"))
      val parts = new_line.split(", ")
      if(parts.length != (numDests)){
        println("Error data because the numDests is wrong!")
        System.exit(-1)
      }
      println(parts.length)
      val litera = line.substring(line.lastIndexOf(",")+1,line.lastIndexOf(")"))
      val y = litera.toDouble
      val data = new Array[Double](parts.length)
      for(i <- 0 until parts.length){
        data(i) = parts(i).toDouble
      }
      val x = DenseVector(data)
      DataPoint(x,y)
    })**/
   val points = sc.objectFile(args(0)).asInstanceOf[RDD[SparkLR.DataPoint]]

    val w = DenseVector.fill(numDests){2*rand.nextDouble() - 1}
    println("Initial w:"+w)

    testOptimized(points,iterations,numDests,w)
    System.gc()
    System.gc()
    System.gc()

    sc.stop()
  }
}