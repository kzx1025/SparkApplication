package flintApp


import java.io.{DataOutputStream, ByteArrayOutputStream}

import org.apache.hadoop.io.WritableComparator
import org.apache.spark._
import org.apache.spark.rdd.{RDD, ShuffledRDD}

/**
 * Created by root on 15-9-25.
 */

class EdgeChunk(size: Int = 4196) extends ByteArrayOutputStream(size) { self =>

  def show() {
    var offset = 0
    while (offset < count) {
      println("src id: " + WritableComparator.readInt(buf, offset))
      offset += 4
      val numDests = WritableComparator.readInt(buf, offset)
      offset += 4
      print("has " + numDests + " dests:")
      var count = 0
      while (count < numDests) {
        print(" " + WritableComparator.readInt(buf, offset))
        offset += 4
        count += 1
      }
      println("")
    }
  }

  def getInitValueIterator(value: Float) = new Iterator[(Int, Float)] {
    var offset = 0

    override def hasNext = offset < self.count

    override def next() = {
      if (!hasNext) Iterator.empty.next()
      else {
        val srcId = WritableComparator.readInt(buf, offset)
        offset += 4
        val numDests = WritableComparator.readInt(buf, offset)
        offset += 4 + 4 * numDests
        (srcId, value)
      }
    }
  }

  def getMessageIterator(vertices: Iterator[(Int, Float)]) = new Iterator[(Int, Float)] {
    var changeVertex = true
    var offset = 0
    var currentDestIndex = 0
    var currentDestNum = 0
    var currentContrib = 0.0f

    private def matchVertices(): Boolean = {
      assert(changeVertex)

      if (offset >= self.count) return false

      var matched = false
      while (!matched && vertices.hasNext) {
        val currentVertex = vertices.next()
        while (currentVertex._1 > WritableComparator.readInt(buf, offset)) {
          offset += 4
          val numDests = WritableComparator.readInt(buf, offset)
          offset += 4 + 4 * numDests

          if (offset >= self.count) return false
        }
        if (currentVertex._1 == WritableComparator.readInt(buf, offset)) {
          matched = true
          offset += 4
          currentDestNum = WritableComparator.readInt(buf, offset)
          offset += 4
          currentDestIndex = 0
          currentContrib = currentVertex._2 / currentDestNum
          changeVertex = false
        }
      }
      matched
    }

    override def hasNext = !changeVertex || matchVertices()

    override def next() = {
      if (!hasNext) Iterator.empty.next()
      else {
        currentDestIndex += 1
        if (currentDestIndex == currentDestNum) changeVertex = true

        val destId = WritableComparator.readInt(buf, offset)
        offset += 4

        (destId, currentContrib)
      }
    }
  }

}

object FlintSparkPR {
  private val ordering = implicitly[Ordering[Int]]

  def testOptimized(groupedEdges: RDD[(Int, Iterable[Int])],iters:Int) {
    val cachedEdges = groupedEdges.mapPartitions ( iter => {
      val chunk = new EdgeChunk
      val dos = new DataOutputStream(chunk)
      for ((src, dests) <- iter) {
        dos.writeInt(src)
        dos.writeInt(dests.size)
        dests.foreach(dos.writeInt)
      }
      Iterator(chunk)
    }, true).cache()

    cachedEdges.foreach(_ => Unit)

    val initRanks = cachedEdges.mapPartitions( iter => {
      val chunk = iter.next()
      chunk.getInitValueIterator(1.0f)
    }, true)

    var ranks = initRanks

    for (i <- 1 to iters) {
      val contribs = cachedEdges.zipPartitions(ranks) { (EIter, VIter) =>
        val chunk = EIter.next()
        chunk.getMessageIterator(VIter)
      }
      ranks = contribs.reduceByKey(cachedEdges.partitioner.get, _ + _).asInstanceOf[ShuffledRDD[Int, _, _]].
        setKeyOrdering(ordering).
        asInstanceOf[RDD[(Int, Float)]].
        mapValues(0.15f + 0.85f * _)
    }
    ranks.foreach(_ => Unit)

  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName(args(2)).setMaster("local")
    val spark = new SparkContext(conf)

    //Logger.getRootLogger.setLevel(Level.FATAL)

    val lines = spark.textFile(args(0))
    val links = lines.map{ s =>
      val parts = s.split("\\s+")
      (parts(0).toInt, parts(1).toInt)
    }.groupByKey().
      asInstanceOf[ShuffledRDD[Int, _, _]].
      setKeyOrdering(ordering).
      asInstanceOf[RDD[(Int, Iterable[Int])]]

    val iters = args(1).toInt
    testOptimized(links,iters)

    spark.stop()
  }
}
