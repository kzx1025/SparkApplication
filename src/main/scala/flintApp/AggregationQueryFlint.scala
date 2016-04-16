package flintApp

import java.util.Arrays
import  java.util.Date
import java.io.{DataOutputStream, ByteArrayOutputStream}

import org.apache.hadoop.io.WritableComparator
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by Administrator on 2016/1/22.
  */
class AggreChunk(X:Int,size:Int=4096)extends  ByteArrayOutputStream(size){self=>
  private  def grow(minCapacity:Int):Unit={
    val oldCapacity: Int = buf.length
    val newCapacity=oldCapacity+4096
    buf=Arrays.copyOf(buf,newCapacity)
  }
  private def ensureCapacity (minCapacity: Int):Unit= {
    if (minCapacity - buf.length > 0)
      grow(minCapacity)
  }
  def getVectorValueIterator()=new Iterator[(String,Float)]{
    var offset=0
    override def hasNext=offset<self.count

    override  def next()={
      if (!hasNext) Iterator.empty.next()
      else {
           val length=WritableComparator.readInt(buf,offset)
          offset+=4
        var Xtemp=X
        if(X>length-1)
          Xtemp=length-1
        val  SourceIp=new String(buf,offset+1,Xtemp)
        offset+=length
        val length2=WritableComparator.readInt(buf,offset)
        offset=offset+4+length2+8
        val adRevenue=WritableComparator.readFloat(buf,offset)
        offset+=4
        for(i<-1 to 4){
          val lengthTemp=WritableComparator.readInt(buf,offset)
          offset=offset+4+lengthTemp
        }
        offset+=4
        (SourceIp,adRevenue)
      }
    }
  }
}
object AggregationQueryFlint{
  def main(args:Array[String]):Unit={
    if(args.length<4){
      System.err.println("Usage of Parameters: inputPath ,X,outputPath ,appName")
      System.exit(1)
    }
    val X=args(1).toInt
    val sparkConf = new SparkConf().setAppName(args(3)).setMaster("local")
    val sparkContext = new SparkContext(sparkConf)
    val file=sparkContext.textFile(args(0)).map(r=>{
      r.split(',').toList
    }).mapPartitions({iter=>
      val Chunk=new AggreChunk(X,4096)
      val dos=new DataOutputStream(Chunk)
      for(list<-iter){
        for(i<-0 to 1) {
          val length = list(i).length
          dos.writeInt(length);
          dos.writeBytes(list(i))
        }
        //the dateString shoud be splited by '-' to make a date object
        val dateString=list(2).split('-')
        dos.writeLong(new Date(dateString(0).toInt,dateString(1).toInt,dateString(2).toInt).getTime)
        dos.writeFloat(list(3).toFloat)
        for(i<-4 to 7){
          val length=list(i).length
          dos.writeInt(length)
          dos.writeBytes(list(i))
        }
        dos.writeInt(list(8).toInt)
         }
      Iterator(Chunk)
    },true).cache()
     val result=file .mapPartitions{iter=>
      val chunk=iter.next()
      val result=chunk.getVectorValueIterator()
      result
    }.reduceByKey(_+_)
    file.saveAsTextFile("result.txt")
    sparkContext.stop()

  }
}
