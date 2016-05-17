package tianchi

import java.io.{File, PrintWriter}
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

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

    val cal:Calendar = Calendar.getInstance()
    val vacationSet = Set("20150404","20150405","20150406","20150501","20150502","20150503","20150620","20150621","20150622")


    def isWeekend(time: String): Int={
      val a:String = new SimpleDateFormat("yyyyMMdd").format(new Date(time.toLong*1000))

      val tempDate:Date = new SimpleDateFormat("yyyyMMdd").parse(a)

      cal.setTime(tempDate)

      if(cal.get(Calendar.DAY_OF_WEEK)==Calendar.SATURDAY||cal.get(Calendar.DAY_OF_WEEK)==Calendar.SUNDAY)
        1
      else
        0
    }

    sqlContext.udf.register("isWeekend",(time: String)=>isWeekend(time))


    def isVacation(time: String): Int={

      val a = new SimpleDateFormat("yyyyMMdd").format(new Date(time.toLong*1000))
      if(isWeekend(time)==1||vacationSet.contains(a))
        1
      else
        0
    }
    sqlContext.udf.register("isVacation",(time: String)=>isVacation(time))

    def isRestTime(time: String): Int={
      val a = new SimpleDateFormat("HHmm").format(new Date(time.toLong*1000)).toLong
      if(a<=1400&&a>=1200||a<=2400&&a>=1900)
        1
      else
        0
    }
    sqlContext.udf.register("isRestTime",(time: String)=>isRestTime(time))

    def isNight(time: String): Int={
      val a = new SimpleDateFormat("HHmm").format(new Date(time.toLong*1000)).toLong
      if(a<=0700&&a>=0000)
        1
      else
        0
    }
    sqlContext.udf.register("isNight",(time: String)=>isNight(time))


   // isVacation("1426406400")


     // "and a.userId='5e4d08ff6f217b64441e856509306f50' and a.songId='964dadbb0bea92365f25f6f411143206'"
   //[userId: string, songId: string, gmtCreate: string, actionType: string, ds: string,
   // songId: string, artistId: string, publishTime: string, songInitPlays: string, language: string, gender: string]

    val sqlString1 = "SELECT userId,songId,count(*) From user_action where ds<20150501 and actionType='1' group by userId,songId"
    //val sqlString2 = "SELECT userId,songId,count(*)  From user_action a JOIN user_song b where a.userId=b.userId and a.songId=b.songId and a.ds<20150501 and a.actionType='1'"
    val userSongRecords1 = sqlContext.sql(sqlString1)


    val sqlString2 = "SELECT userId,songId,count(*) From user_action where ds<20150501 and actionType='2' group by userId,songId"
    //val sqlString2 = "SELECT userId,songId,count(*)  From user_action a JOIN user_song b where a.userId=b.userId and a.songId=b.songId and a.ds<20150501 and a.actionType='1'"
    val userSongRecords2 = sqlContext.sql(sqlString2)
  //  userSongRecords2.registerTempTable("user_song_download")

    val sqlString3 = "SELECT userId,songId,count(*) From user_action where ds<20150501 and actionType='3' group by userId,songId"
    //val sqlString2 = "SELECT userId,songId,count(*)  From user_action a JOIN user_song b where a.userId=b.userId and a.songId=b.songId and a.ds<20150501 and a.actionType='1'"
    val userSongRecords3 = sqlContext.sql(sqlString3)
  //  userSongRecords3.registerTempTable("user_song_collect")

    val sqlString7 = "SELECT userId,songId,count(*) From user_action where ds<20150501 and isVacation(gmtCreate)=1 and actionType='1' group by userId,songId"
    val vacationRecords = sqlContext.sql(sqlString7)

    val sqlString8= "SELECT userId,songId,count(*) From user_action where ds<20150501 and isRestTime(gmtCreate)=1 and actionType='1' group by userId,songId"
    val restRecords = sqlContext.sql(sqlString8)

    val sqlString11= "SELECT userId,songId,count(*) From user_action where ds<20150501 and isNight(gmtCreate)=1 and actionType='1' group by userId,songId"
    val nightRecords = sqlContext.sql(sqlString11)



    userSongRecords1.registerTempTable("user_song_play")
    val sqlString4 = "select a.*,isVacation(a.gmtCreate),isRestTime(a.gmtCreate),isNight(a.gmtCreate),toInt(b.c2) From user_action a left join user_song_play b ON a.userId=b.userId and a.songId=b.songId where a.ds<20150701 and a.ds>=20150501"
    val records4 = sqlContext.sql(sqlString4)
    sqlContext.dropTempTable("user_song_play")
    records4.registerTempTable("temp_table1")


    userSongRecords2.registerTempTable("user_song_download")
    val sqlString5 = "select a.*,toInt(b.c2) From temp_table1 a left join user_song_download b ON a.userId=b.userId and a.songId=b.songId where a.ds<20150701 and a.ds>=20150501"
    val records5 = sqlContext.sql(sqlString5)
    sqlContext.dropTempTable("user_song_download")
    records5.registerTempTable("temp_table2")

    userSongRecords3.registerTempTable("user_song_collect")
    val sqlString6 = "select a.*,toInt(b.c2) From temp_table2 a left join user_song_collect b ON a.userId=b.userId and a.songId=b.songId where a.ds<20150701 and a.ds>=20150501"
    val records6 = sqlContext.sql(sqlString6)
    sqlContext.dropTempTable("user_song_collect")
    records6.registerTempTable("temp_table3")

    vacationRecords.registerTempTable("user_song_vacation")
    val sqlString9 = "select a.*,toInt(b.c2) From temp_table3 a left join user_song_vacation b ON a.userId=b.userId and a.songId=b.songId where a.ds<20150701 and a.ds>=20150501"
    val records9 = sqlContext.sql(sqlString9)
    sqlContext.dropTempTable("user_song_vacation")
    records9.registerTempTable("temp_table4")

    restRecords.registerTempTable("user_song_rest")
    val sqlString10 = "select a.*,toInt(b.c2) From temp_table4 a left join user_song_rest b ON a.userId=b.userId and a.songId=b.songId where a.ds<20150701 and a.ds>=20150501"
    val records10 = sqlContext.sql(sqlString10)
    sqlContext.dropTempTable("user_song_rest")
    records10.registerTempTable("temp_table5")

    nightRecords.registerTempTable("user_song_night")
    val sqlString12 = "select a.*,toInt(b.c2) From temp_table5 a left join user_song_night b ON a.userId=b.userId and a.songId=b.songId where a.ds<20150701 and a.ds>=20150501"
    val records12 = sqlContext.sql(sqlString12)
    sqlContext.dropTempTable("user_song_night")
    records12.registerTempTable("temp_table6")

    val sqlString13 = "select a.*,b.artistId,b.publishTime,b.songInitPlays,b.language,b.gender From temp_table6 a left join song b On a.songId=b.songId order by a.userId"
    val finalRecords = sqlContext.sql(sqlString13)



    val writer = new PrintWriter(new File(args(3)))
  //  writer.write(records4.collect().length.toString)
    for(record<- finalRecords.collect()) {
      var i = 0
      for (row <- record.toSeq) {
        if(i!=0) {
          writer.write(",")
        }


        writer.write(row.toString)

        i+=1
    }
      writer.println()

    }

    writer.close()
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

  }
}
