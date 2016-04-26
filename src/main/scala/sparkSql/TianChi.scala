package sparkSql

import java.io.PrintWriter

import java.io._
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable.Queue

/**
  * Created by iceke on 16/4/16.
  */
object TianChi {
  case class UserAction(userId: String, songId: String, gmtCreate: String, actionType: String, ds: String)
  case class Song(songId: String, artistId: String, publishTime: String, songInitPlays: String,
                  language: String, gender: String)

  def main(args: Array[String]): Unit ={
    if(args.length<3){
      System.err.println("Usage of Parameters: master userInput songInput outputPath")
      System.exit(1)
    }
    val sparkConf = new SparkConf()
      .setMaster(args(0))
      .setAppName("TianChi")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    val user_action = sc.textFile(args(1)).map(_.split(","))
      .filter(line => line.length == 5)
      .map(a => UserAction(a(0),a(1),a(2),a(3),a(4)))
    val userSchema = sqlContext.createDataFrame(user_action)
    userSchema.registerTempTable("user_action")

    val song = sc.textFile(args(2)).map(_.split(","))
      .filter(line => line.length == 6)
      .map(a => Song(a(0),a(1),a(2),a(3),a(4),a(5)))
    val songSchema = sqlContext.createDataFrame(song)
    songSchema.registerTempTable("song")

    def toInt(s:String): Int={
      try{
        s.toInt
      } catch {
        case e:Exception => 9999
      }
    }


    def statPlayTimes(time: String): String={
      val tempSql = "SELECT ds,count(ds) FROM sub_song WHERE actionType='1' " +
        "and ds="+time+" group by ds"
      val result = sqlContext.sql(tempSql)

      var totalTimes = 0
      for(a <- result.collect()){
        totalTimes+=a(1).toString.toInt
      }
      totalTimes.toString
    }

    def statDownloadTimes(time: String): String={
      val tempSql = "SELECT ds,count(ds) FROM sub_song WHERE actionType='2' " +
        "and ds="+time+" group by ds"
      val result = sqlContext.sql(tempSql)

      var totalTimes = 0
      for(a <- result.collect()){
        totalTimes+=a(1).toString.toInt
      }
      totalTimes.toString
    }

    def statCollectTimes(time: String): String={
      val tempSql = "SELECT ds,count(ds) FROM sub_song WHERE actionType='3' " +
        "and ds="+time+" group by ds"
      val result = sqlContext.sql(tempSql)

      var totalTimes = 0
      for(a <- result.collect()){
        totalTimes+=a(1).toString.toInt
      }
      totalTimes.toString
    }

    //统计歌曲的播放次数 并排序
    val sqlString1 = "SELECT songId, count(songId) FROM user_action WHERE actionType='1' group by songId order by songId"


    sqlContext.udf.register("statHistoryTimes",(time: String)=>statPlayTimes(time))
    //得到所有的ds
    val sqlString2 = "SELECT ds FROM user_action group by ds order by ds"
    val allDS = sqlContext.sql(sqlString2)

    //筛选出特定songId的子表
    val sqlString3 = "SELECT * FROM user_action a JOIN song b ON a.songId=b.songId WHERE b.artistId='8da51d03b8b8717431e8b902856fb45e'"
    val subSongTable = sqlContext.sql(sqlString3)
    subSongTable.registerTempTable("sub_song")


    //查询初始热度
    val sqlString4 = "SELECT songInitPlays FROM song WHERE songId='7ec488fc483386cdada5448864e82990'"
    val initPlays = sqlContext.sql(sqlString4)
    initPlays.collect()(0)(0).toString


  //  result.registerTempTable("song_times")
   // val result2 = sqlContext.sql("SELECT songId,c1 From song_times where songId='ec488fc483386cdada5448864e82990'")
  val writer = new PrintWriter(new File(args(3)))
    var totalPlayTimes = 0
    var totalDownloadTimes = 0
    var totalCollectTimes = 0
    var sevenPlayTimes = 0
    var sevenDownloadTimes = 0
    var sevenCollectTimes = 0
    val playQueue = Queue[Int]()
    val downloadQueue = Queue[Int]()
    val collectQueue = Queue[Int]()
    var days = 0
    for(ds <- allDS.collect()){
      totalPlayTimes+=statPlayTimes(ds(0).toString).toInt
      totalDownloadTimes+=statDownloadTimes(ds(0).toString).toInt
      totalCollectTimes+=statCollectTimes(ds(0).toString).toInt

      if(days<7){
        sevenPlayTimes = totalPlayTimes
        sevenDownloadTimes = totalDownloadTimes
        sevenCollectTimes = totalCollectTimes
        playQueue.enqueue(totalPlayTimes)
        downloadQueue.enqueue(totalDownloadTimes)
        collectQueue.enqueue(totalCollectTimes)

      }else{
        val beforeSevenPlays = playQueue.dequeue
        val beforeSevenDownloads = downloadQueue.dequeue
        val beforeSevenCollects = collectQueue.dequeue
        sevenPlayTimes = totalPlayTimes - beforeSevenPlays
        sevenDownloadTimes = totalDownloadTimes - beforeSevenDownloads
        sevenCollectTimes = totalCollectTimes - beforeSevenCollects
        playQueue.enqueue(totalPlayTimes)
        downloadQueue.enqueue(totalDownloadTimes)
        collectQueue.enqueue(totalCollectTimes)
      }

      println(ds(0).toString+","+totalPlayTimes+","+totalDownloadTimes+","+totalCollectTimes+","+sevenPlayTimes
        +","+sevenDownloadTimes+","+sevenCollectTimes+","+initPlays.collect()(0)(0).toString)
      writer.write(ds(0).toString+","+totalPlayTimes+","+totalDownloadTimes+","+totalCollectTimes+","+sevenPlayTimes
        +","+sevenDownloadTimes+","+sevenCollectTimes+","+initPlays.collect()(0)(0).toString)
      writer.println()
      days+=1
    }
    writer.close()

    //write to file
/*    val writer = new PrintWriter(new File(args(3)))
    for(a <- result.collect()){
      writer.write(a(0).toString+","+a(1).toString)
      writer.println()
    }
    writer.close()*/

  }

  def statSongInfo():Unit={

  }

}
