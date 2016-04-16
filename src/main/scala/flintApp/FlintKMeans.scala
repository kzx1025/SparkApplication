package flintApp

import java.io.{DataOutputStream, ByteArrayOutputStream}

import breeze.linalg.{Vector, DenseVector}
import org.apache.hadoop.io.WritableComparator
import org.apache.spark.{SparkContext, SparkConf}


/**
 * Created by iceke on 15/10/12.
 */

class KChunk(K: Int,dimensions: Int,size: Int=4196)extends ByteArrayOutputStream(size){ self =>
  def getVectorValueIterator() = new Iterator[Array[Double]] {
    var offset = 0
    var currentPoint=new Array[Double](dimensions)
    var i = 0

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
        //calculate the dot value
        currentPoint.clone()
      }
    }
  }
}

object FlintKMeans {


  def distPoint(a: Array[Double], b:Array[Double]): Double ={
    if(a.length!=b.length){
      System.err.println("the point's dimension is not equal")
      System.exit(1)
    }
    var sum = 0.0
    for(i<-0 until a.length){
      sum+=(a(i)-b(i))*(a(i)-b(i))
    }
    math.sqrt(sum)
  }

  def closestPoint(p: Array[Double], centers: Array[Array[Double]]): Int = {
    var bestIndex = 0
    var closest = Double.PositiveInfinity
    for(i<-0 until centers.length){
      val tempDist = distPoint(p, centers(i))
      if (tempDist < closest) {
        closest = tempDist
        bestIndex = i
      }
    }

    bestIndex
  }

  def showWarning() {
    System.err.println(
      """WARN: This is a naive implementation of KMeans Clustering and is given as an example!
        |Please use the KMeans method found in org.apache.spark.mllib.clustering
        |for more conventional use.
      """.stripMargin)
  }

  def main(args: Array[String]): Unit ={
    if (args.length < 5) {
      System.err.println("Usage: FlintKMeans <file> <k> <convergeDist> <appName> <dimensions>")
      System.exit(1)
    }
    showWarning()
    val sparkConf = new SparkConf().setAppName(args(4))
    val sc = new SparkContext(sparkConf)
    val lines = sc.textFile(args(0))
    val convergeDist = args(2).toDouble
    val final_lines = lines.map(line=>{
      val parts = line.split(' ')
      val data = new Array[Double](parts.length)
      for(i<-0 until parts.length){
        data(i) = parts(i).toDouble
      }
      DenseVector(data).toArray
    }
    )
    val K = args(1).toInt
    val dimensions = args(5).toInt
    val data = final_lines.mapPartitions({ iter =>
      val (iterOne, iterTwo) = iter.duplicate
      val chunk = new KChunk(K,dimensions,8*iterOne.length*dimensions)
      val dos = new DataOutputStream(chunk)
      for (point <- iterTwo) {
        point.foreach(dos.writeDouble)
      }
      Iterator(chunk)
    },true).cache()

    val kPoints = final_lines.takeSample(withReplacement = false,K,42).toArray

    var tempDist = 1.0
    val tempPoints :Array[Array[Double]] = new Array(K)

    while(tempDist > convergeDist){
      val closet = data.mapPartitions{iter =>
        val chunk = iter.next()
        val tempPoint = chunk.getVectorValueIterator()
        for(p <- tempPoint){
          val index = closestPoint(p,kPoints)
        }


        tempPoint
      }


    }


  }

}
