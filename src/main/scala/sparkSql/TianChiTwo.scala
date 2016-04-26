package sparkSql

import java.io.{File, PrintWriter}

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}
import sparkSql.TianChiTwo.{Song, UserAction}

/**
  * Created by iceke on 16/4/24.
  */
object TianChiTwo {
  case class UserAction(userId: String, songId: String, gmtCreate: String, actionType: String, ds: String)
  case class Song(songId: String, artistId: String, publishTime: String, songInitPlays: String,
                  language: String, gender: String)

  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      System.err.println("Usage of Parameters: master userInput songInput outputPath")
      System.exit(1)
    }
    val sparkConf = new SparkConf()
      .setMaster(args(0))
      .setAppName("TianChi")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.executor.memory", "3g")
      .set("spark.driver.memory", "1g")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    val user_action = sc.textFile(args(1)).map(_.split(","))
      .filter(line => line.length == 5)
      .map(a => UserAction(a(0), a(1), a(2), a(3), a(4)))
    val userSchema = sqlContext.createDataFrame(user_action)
    userSchema.registerTempTable("user_action")

    val song = sc.textFile(args(2)).map(_.split(","))
      .filter(line => line.length == 6)
      .map(a => Song(a(0), a(1), a(2), a(3), a(4), a(5)))
    val songSchema = sqlContext.createDataFrame(song)
    songSchema.registerTempTable("song")

    def toInt(s: Long): Int = {
      try {
        s.toInt
      } catch {
        case e: Exception => 0
      }
    }
    sqlContext.udf.register("toInt",(time: Long)=>toInt(time))

    def isVacation(time: String): Int={
     val a = new java.text.SimpleDateFormat("yyyy MM-dd HH:mm:ss").format(new java.util.Date(time.toLong*1000))
      println(a)
      0
    }

    isVacation("1426406400")

    def firstPlayTimes(userId:String,songId:String): String={
      val tempSql = "SELECT ds FROM sub_song WHERE actionType='1' order by ds limit 1"
      val result = sqlContext.sql(tempSql)
      result.collect()(0).toString()
    }

    val writer = new PrintWriter(new File(args(3)))
     // "and a.userId='5e4d08ff6f217b64441e856509306f50' and a.songId='964dadbb0bea92365f25f6f411143206'"
   //[userId: string, songId: string, gmtCreate: string, actionType: string, ds: string,
   // songId: string, artistId: string, publishTime: string, songInitPlays: string, language: string, gender: string]

    val sqlString1 = "SELECT userId,songId,count(*) From user_action where ds<20150501 and actionType='1' group by userId,songId order by userId"
    //val sqlString2 = "SELECT userId,songId,count(*)  From user_action a JOIN user_song b where a.userId=b.userId and a.songId=b.songId and a.ds<20150501 and a.actionType='1'"
    val userSongRecords1 = sqlContext.sql(sqlString1)


    val sqlString2 = "SELECT userId,songId,count(*) From user_action where ds<20150501 and actionType='2' group by userId,songId order by userId"
    //val sqlString2 = "SELECT userId,songId,count(*)  From user_action a JOIN user_song b where a.userId=b.userId and a.songId=b.songId and a.ds<20150501 and a.actionType='1'"
    val userSongRecords2 = sqlContext.sql(sqlString2)
  //  userSongRecords2.registerTempTable("user_song_download")

    val sqlString3 = "SELECT userId,songId,count(*) From user_action where ds<20150501 and actionType='3' group by userId,songId order by userId"
    //val sqlString2 = "SELECT userId,songId,count(*)  From user_action a JOIN user_song b where a.userId=b.userId and a.songId=b.songId and a.ds<20150501 and a.actionType='1'"
    val userSongRecords3 = sqlContext.sql(sqlString3)
  //  userSongRecords3.registerTempTable("user_song_collect")



    userSongRecords1.registerTempTable("user_song_play")
    val sqlString4 = "select a.*,toInt(b.c2) From user_action a left join user_song_play b ON a.userId=b.userId and a.songId=b.songId where a.ds<20150510 and a.ds>=20150501"
    val records4 = sqlContext.sql(sqlString4)
    sqlContext.dropTempTable("user_song_play")
    records4.registerTempTable("temp_table1")


    userSongRecords2.registerTempTable("user_song_download")
    val sqlString5 = "select a.*,toInt(b.c2) From temp_table1 a left join user_song_download b ON a.userId=b.userId and a.songId=b.songId where a.ds<20150510 and a.ds>=20150501"
    val records5 = sqlContext.sql(sqlString5)
    sqlContext.dropTempTable("user_song_download")
    records5.registerTempTable("temp_table2")

    userSongRecords3.registerTempTable("user_song_collect")
    val sqlString6 = "select a.*,toInt(b.c2) From temp_table2 a left join user_song_collect b ON a.userId=b.userId and a.songId=b.songId where a.ds<20150510 and a.ds>=20150501"
    val records6 = sqlContext.sql(sqlString6)
    sqlContext.dropTempTable("user_song_collect")
    records6.registerTempTable("user_times")

  //  writer.write(records4.collect().length.toString)
    for(record<- records5.collect()){
      writer.write(record.toString())
      writer.println()

    }

















   /* for(record<-records4.collect()){
      writer.write(record.toString())
      writer.println()
    }
*/


/*
    for(user <- userRecords.collect()) {
      for (song <- songRecords.collect()) {

        val tempSql = "SELECT * From user_action a JOIN song b ON a.songId=b.songId where a.userId='"+user(0).toString+"' and a.songId='"+song(0).toString+"' order by ds"
        val subRecords = sqlContext.sql(tempSql)
        subRecords.registerTempTable("sub_song")



        for (record <- subRecords.collect()) {
          println(record.toString)
          val havePlay = havePlayTimes(record(0).toString, record(1).toString, record(4).toString)
          writer.write(record(0) + "," + havePlay)

        }
      }
    }*/
    writer.close()



  }
}
