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

    def toInt(s: String): Int = {
      try {
        s.toInt
      } catch {
        case e: Exception => 9999
      }
    }


    def havePlayTimes(userId:String,songId:String,ds:String): String={
      val tempSql = "SELECT count(*) FROM sub_song WHERE actionType='1' " +
        "and ds<="+ds
      val result = sqlContext.sql(tempSql)
      result.collect()(0).toString()
    }


    val sqlString1 = "SELECT  * FROM user_action a  JOIN song b on a.songId=b.songId"
     // "and a.userId='5e4d08ff6f217b64441e856509306f50' and a.songId='964dadbb0bea92365f25f6f411143206'"
   //[userId: string, songId: string, gmtCreate: string, actionType: string, ds: string,
   // songId: string, artistId: string, publishTime: string, songInitPlays: string, language: string, gender: string]
    val allRecords = sqlContext.sql(sqlString1)

    val sqlString2 = "SELECT userId FROM user_action group by userId"
    val userRecords = sqlContext.sql(sqlString2)
   // println(userRecords.collect().length)

    val sqlString3 = "SELECT songId FROM song group by songId"
    val songRecords = sqlContext.sql(sqlString3)
    println(songRecords.collect().length)

    val sqlString4 = "SELECT userId,songId,ds FROM user_action as a where ds=(SELECT ds From user_action as b where a.userId=b.userId and a.songId=b.songId order by ds limit 1)"
    val firstDsRecords = sqlContext.sql(sqlString4)





    val writer = new PrintWriter(new File(args(3)))
    for(firstDs <- firstDsRecords.collect()){
      writer.write(firstDs.toString())
    }

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
