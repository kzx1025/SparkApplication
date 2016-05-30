package sparkSql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}
import tianchi.TianChiLineXl.adn

import scala.collection.mutable.Queue

/**
  * Created by iceke on 16/4/19.
  */
object SomeTest {
  case class  adn(artistId:String,ds:String,num:String)
  def main(args: Array[String]): Unit ={
    if(args.length<1){
      System.err.println("Usage of Parameters: tianchi_user.csv songsIntid.csv  day.csv  disUs.csv   outputyb ")
      System.exit(1)
    }
    val sparkConf = new SparkConf()
      .setAppName("LineYc")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .setMaster("local").set("spark.executor.memory","2g")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    val adnt = sc.textFile(args(0))
      .map(_.split(","))
      .filter(line => line.length == 3)
      .map(a => adn(a(0), a(1), a(2)))
    val adnScheme = sqlContext.createDataFrame(adnt)
    adnScheme.registerTempTable("adn")

    val a = sqlContext.sql("select count(*) from adn group by num")
    a.collect().foreach(println)
  }

}
