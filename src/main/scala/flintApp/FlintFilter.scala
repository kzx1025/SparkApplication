package flintApp

import java.io.{DataOutputStream, ByteArrayOutputStream}

import org.apache.hadoop.io.WritableComparator
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable.ArrayBuffer

/**
 * Created by root on 15-9-26.
 */

class WordChunk extends ByteArrayOutputStream{ self =>
  def getFilter(regex:Int)= new Iterator[(String,Int)]{
    var offset = 0

    var i = 0

    var currentKey:String = null
    var currentvalue:Int = 0

    override def hasNext= {
      if (offset < self.count) {

        var resultl: String = null
        var matched: Boolean = false
        var end = false
        var rank: Int = 0
        while (!matched && !end) {

          val urlLength = WritableComparator.readInt(buf, offset)
          offset += urlLength+4
          currentvalue = WritableComparator.readInt(buf, offset)

          if (currentvalue > regex) {
            currentKey = new String(buf, offset - urlLength, urlLength)
            matched = true
          }
          offset += 4
          if(offset >= self.count){
            end = true
          }

        }
      matched
      }
        else{
          false
        }
      }


    override def next()= {

      if (currentKey=="" && currentvalue==0) Iterator.empty.next()
      else {
        //read data from the chunk
        (currentKey,currentvalue)
      }

    }
  }
}

object FlintFilter {
  def
  main(args:Array[String]){
    val sparkConf = new SparkConf().setAppName(args(3)).setMaster("local")
    val sparkContext = new SparkContext(sparkConf)
    val lines = sparkContext.textFile(args(0))
    var max_length = 0

    val words = lines.map{s=>
      val parts = s.split("\\s+")
      (parts(0),parts(1).toInt,parts(2).toInt)
    }

    val chunkWords = words.mapPartitions ( iter => {
      val chunk = new WordChunk
      val dos = new DataOutputStream(chunk)
      for ((url, rank, x) <- iter) {
        dos.writeInt(url.length)
        dos.writeBytes(url)
        dos.writeInt(rank)
      }
      Iterator(chunk)
    }, true).cache()

      chunkWords.foreach(_ => Unit)
    val finalWords = chunkWords.mapPartitions( iter => {
      val chunk = iter.next()
      chunk.getFilter(args(2).toInt)
    })

  finalWords.saveAsTextFile(args(1))
    //finalWords.foreach(t => println(t))
  }
}