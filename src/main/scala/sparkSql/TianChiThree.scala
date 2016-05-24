package sparkSql

import java.io.{File, PrintWriter}
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import sparkSql.TianChiTwo.UserAction
import sparkSql.TianChiTwo.Song

/*
case class UserAction(userId: String, songId: String, gmtCreate: String, actionType: String, ds: String)
case class Song(songId: String, artistId: String, publishTime: String, songInitPlays: String,
                language: String, gender: String)
*/

/**
  * Created by feiwang on 2016/5/12.
  */
object TianChiThree {
  case class usdn(userId:String,songId:String,ds:String,num:String)
  case class  userall22(userId:String,ubf1:String,ubf2:String,ubf3:String,ubf4:String,ubf5:String,ubf6:String,ubf7:String,
                         usc1:String,usc2:String,usc3:String,usc4:String,usc5:String,usc6:String,usc7:String,
                         uxz1:String,uxz2:String,uxz3:String,uxz4:String,uxz5:String,uxz6:String,uxz7:String
                         )
  case class userall4(userId:String,ubInva:String,usInva:String,uxInva:String)

  case class songall(songId:String,IntId:String,publishTime:String,songInitPlays:String,language:String,gender:String,
                        sbf1:String,sbf2:String,sbf3:String,sbf4:String,sbf5:String,sbf6:String,sbf7:String,
                        ssc1:String,ssc2:String,ssc3:String,ssc4:String,ssc5:String,ssc6:String,ssc7:String)
  case class songallhou(songId:String,sxz1:String,sxz2:String,sxz3:String,sxz4:String,sxz5:String,sxz6:String,sxz7:String,
                        sbfInva:String,sscInva:String,sxzInva:String)
  case class SongIntId(IntId:String,songId: String, artistId: String, publishTime: String, songInitPlays: String,
                  language: String, gender: String)
  case class dayall(ds:String,weekday:String,isVa:String)
  case  class usn(userId:String,songId:String,num:String)
  def main(args:Array[String]):Unit={
    if (args.length < 13) {
      System.err.println("Usage of Parameters: master userInput songIntId usdn56 user309all song103all day56 day34 qbf qsc qxz hbf  output0 output1")
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

    val users = sc.textFile(args(1)).map(_.split(","))
      .filter(line => line.length == 5)
      .map(a => UserAction(a(0), a(1), a(2),a(3),a(4)))
     val userScheme =sqlContext.createDataFrame(users)
    userScheme.registerTempTable("tianchi_users")

    val songs = sc.textFile(args(2)).map(_.split(","))
      .filter(line => line.length ==6)
      .map(a=> SongIntId(a(0), a(1), a(2), a(3), a(4), a(5),a(6)))
    val songScheme=sqlContext.createDataFrame(songs)
    songScheme.registerTempTable("songIntId")

    val usdn56=sc.textFile(args(3)).map(_.split(","))
      .filter(line=>line.length==4)
      .map(a=>usdn(a(0), a(1), a(2),a(3)))
    val usdn56Scheme=sqlContext.createDataFrame(usdn56)
    usdn56Scheme.registerTempTable("usdn56")

    val user309allqian =sc.textFile(args(4)).map(_.split(","))
      .filter(line=>line.length==25)
      .map(a=>userall22(a(0), a(1), a(2), a(3), a(4), a(5),a(6),a(7),a(8),a(9),a(10),a(11),a(12),a(13),
        a(14),a(15),a(16),a(17),a(18),a(19),a(20),a(21)))
    val userallSchemeqian=sqlContext.createDataFrame(user309allqian)
    userallSchemeqian.registerTempTable("user309allqian")

    val user309allhou=sc.textFile(args(4)).map(_.split(","))
      .filter(line=>line.length==25)
      .map(a=>userall4(a(0),a(22),a(23),a(24)))
    val userallSchemehou=sqlContext.createDataFrame(user309allhou)
    userallSchemehou.registerTempTable("user309allhou")

    val sqlString0001="select a.*,b.ubInva,b.usInva,b.uxInva from user309allqian a join user309allhou b on a.userId=b.userId"
    val record0001=sqlContext.sql(sqlString0001)
    record0001.registerTempTable("user309all")




    val song103all=sc.textFile(args(5)).map(_.split(","))
      .filter(line=>line.length==30)
      .map(a=>songall(a(0), a(1), a(2), a(3), a(4), a(5),a(6),a(7),a(8),a(9),a(10),a(11),a(12),a(13),
        a(14),a(15),a(16),a(17),a(18),a(19)))
    val songallScheme=sqlContext.createDataFrame(song103all)
    songallScheme.registerTempTable("song103allqian")

    val song103allhou=sc.textFile(args(5)).map(_.split(","))
      .filter(line=>line.length==30)
      .map(a=>songallhou(a(0),a(20),a(21),a(22),a(23),a(24),a(25),a(26),a(27),a(28),a(29)))
    val songallSchemeHou=sqlContext.createDataFrame(song103allhou)
    songallSchemeHou.registerTempTable("song103allhou")

    val sqlString0002 = "select a.*,b.sxz1,b.sxz2,b.sxz3,b.sxz4,b.sxz5,b.sxz6,b.sxz7,b.sbfInva,b.sscInva,b.sxzInva from song103allqian a join " +
      "song103allhou b on a.songId=b.songId"
    val record0002=sqlContext.sql(sqlString0002)
    record0002.registerTempTable("song103all")




    val day56=sc.textFile(args(6)).map(_.split(","))
      .filter(line=>line.length==3)
      .map(a=>dayall(a(0), a(1), a(2)))
    val day56Scheme=sqlContext.createDataFrame(day56)
    day56Scheme.registerTempTable("day56")

    val day34=sc.textFile(args(7)).map(_.split(","))
      .filter(line=>line.length==3)
      .map(a=>dayall(a(0), a(1), a(2)))
    val day34Scheme=sqlContext.createDataFrame(day34)
    day34Scheme.registerTempTable("day34")


    val qbf=sc.textFile(args(8)).map(_.split(","))
      .filter(line=>line.length==3).map(a=>usn(a(0),a(1),a(2)))
    val qbfScheme=sqlContext.createDataFrame(qbf)
    qbfScheme.registerTempTable("qbf")

    val qsc=sc.textFile(args(9)).map(_.split(","))
      .filter(line=>line.length==3).map(a=>usn(a(0),a(1),a(2)))
    val qscScheme=sqlContext.createDataFrame(qsc)
    qscScheme.registerTempTable("qsc")

    val qxz=sc.textFile(args(10)).map(_.split(","))
      .filter(line=>line.length==3).map(a=>usn(a(0),a(1),a(2)))
    val qxzScheme=sqlContext.createDataFrame(qxz)
    qxzScheme.registerTempTable("qxz")

    val hbf=sc.textFile(args(11)).map(_.split(","))
      .filter(line=>line.length==3).map(a=>usn(a(0),a(1),a(2)))
    val hbfScheme=sqlContext.createDataFrame(hbf)
    hbfScheme.registerTempTable("hbf")










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

    def isNull(action_num :String):String={
      if (action_num == null){
        "0"
      }
      else action_num
    }
    sqlContext.udf.register("isNull",(action_num:String)=> isNull(action_num))
    //xian chuli zhengyangben

    val sqlString1 ="select distinct userId from usdn56"
    val sqlString2="select distinct songId from usdn56"
    val record1=sqlContext.sql(sqlString1)
    record1.registerTempTable("user56")
    val record2=sqlContext.sql(sqlString2)
    record2.registerTempTable("song56")

   val  sqlString3="select * from tianchi_users where ds< 20150501"
    val record3=sqlContext.sql(sqlString3)
    record3.registerTempTable("useraction34")
    val sqlString4="select a.*,b.weekday,b.isVa from useraction34 a join day34 b on a.ds=b.ds"
    val record4=sqlContext.sql(sqlString4)
    record4.registerTempTable("ua34JoinDay")

    //yonghu tezheng yonghuqiangliangyuecishu   zhouyiersansiwuliu  jaiqibf sc xz
    val sqlString5="select userId,actionType,count(*)as num from ua34JoinDay  group by userId,actionType"
    val record5=sqlContext.sql(sqlString5)
    record5.registerTempTable("ua34UA") ///zuihouzaijiashang*************************8

    val sqlString6="select userId,weekday,count(*) as num from ua34JoinDay where actionType='1' group by userId,weekday"
    val record6=sqlContext.sql(sqlString6)
    record6.registerTempTable("ua34Bfweek")

    val sqlString7="select userId,weekday,count(*) as num from ua34JoinDay where actionType='2' group by userId,weekday"
    val record7=sqlContext.sql(sqlString7)
    record7.registerTempTable("ua34SCweek")

    val sqlString8="select userId,weekday,count(*) as num from ua34JoinDay where actionType='3' group by userId,weekday"
    val record8=sqlContext.sql(sqlString8)
    record8.registerTempTable("ua34XZweek")

    val sqlString9="select userId,actionType,count(*)as num from ua34JoinDay where isVa='1' group by userId,actionType"
    val record9=sqlContext.sql(sqlString9)
    record9.registerTempTable("ua34ACInva")

    val sqlString10="select a.*,b.num  as ubf1  from user56 a left join (select userId,num from ua34Bfweek where weekday='1')" +
      " b on a.userId=b.userId"
    val record10=sqlContext.sql(sqlString10)
    record10.registerTempTable("ub1")

    val sqlString11 ="select a.*,b.num as ubf2 from ub1 a left join (select userId,num from ua34Bfweek where weekday='2')" +
      "b on a.userId=b.userId"
    val record11=sqlContext.sql(sqlString11)
    record11.registerTempTable("ub2")

    val sqlString12="select a.*,b.num as ubf3 from ub2 a left join(select userId,num from ua34Bfweek where weekday='3')" +
      "b on a.userId=b.userId"
    val record12=sqlContext.sql(sqlString12)
    record12.registerTempTable("ub3")

    val sqlString13="select a.* ,b.num as ubf4 from ub3 a left join (select userId,num from ua34Bfweek where weekday='4') " +
      "b on a.userId=b.userId"
    val record13=sqlContext.sql(sqlString13)
    record13.registerTempTable("ub4")

    val sqlString14="select a.*,b.num as ubf5 from ub4 a left join (select userId,num from ua34Bfweek where weekday='5')" +
      "b on a.userId=b.userId"
    val record14=sqlContext.sql(sqlString14)
    record14.registerTempTable("ub5")

    val sqlString15="select a.*,b.num as ubf6 from ub5 a left join (select userId,num from ua34Bfweek where weekday='6')" +
      "b on a.userId=b.userId"
    val record15=sqlContext.sql(sqlString15)
    record15.registerTempTable("ub6")

    val sqlString16="select a.*,b.num as ubf7 from ub6  a left join (select userId,num from ua34Bfweek where weekday='7')" +
      "b on a.userId=b.userId"
    val record16=sqlContext.sql(sqlString16)
    record16.registerTempTable("ub7")

    val sqlString17="select a.*,b.num  as usc1  from ub7 a left join (select userId,num from ua34SCweek where weekday='1')" +
      " b on a.userId=b.userId"
    val record17=sqlContext.sql(sqlString17)
    record17.registerTempTable("us1")

    val sqlString18 ="select a.*,b.num as usc2 from us1 a left join (select userId,num from ua34SCweek where weekday='2')" +
      "b on a.userId=b.userId"
    val record18=sqlContext.sql(sqlString18)
    record18.registerTempTable("us2")

    val sqlString19="select a.*,b.num as usc3 from us2 a left join(select userId,num from ua34SCweek where weekday='3')" +
      "b on a.userId=b.userId"
    val record19=sqlContext.sql(sqlString19)
    record19.registerTempTable("us3")

    val sqlString20="select a.* ,b.num as usc4 from us3 a left join (select userId,num from ua34SCweek where weekday='4') " +
      "b on a.userId=b.userId"
    val record20=sqlContext.sql(sqlString20)
    record20.registerTempTable("us4")

    val sqlString21="select a.*,b.num as usc5 from us4 a left join (select userId,num from ua34SCweek where weekday='5')" +
      "b on a.userId=b.userId"
    val record21=sqlContext.sql(sqlString21)
    record21.registerTempTable("us5")

    val sqlString22="select a.*,b.num as usc6 from us5 a left join (select userId,num from ua34SCweek where weekday='6')" +
      "b on a.userId=b.userId"
    val record22=sqlContext.sql(sqlString22)
    record22.registerTempTable("us6")

    val sqlString23="select a.*,b.num as usc7 from us6  a left join (select userId,num from ua34SCweek where weekday='7')" +
      "b on a.userId=b.userId"
    val record23=sqlContext.sql(sqlString23)
    record23.registerTempTable("us7")


    val sqlString24="select a.*,b.num  as uxz1  from us7 a left join (select userId,num from ua34XZweek where weekday='1')" +
      " b on a.userId=b.userId"
    val record24=sqlContext.sql(sqlString24)
    record24.registerTempTable("ux1")

    val sqlString25 ="select a.*,b.num as uxz2 from ux1 a left join (select userId,num from ua34XZweek where weekday='2')" +
      "b on a.userId=b.userId"
    val record25=sqlContext.sql(sqlString25)
    record25.registerTempTable("ux2")

    val sqlString26="select a.*,b.num as uxz3 from ux2 a left join(select userId,num from ua34XZweek where weekday='3')" +
      "b on a.userId=b.userId"
    val record26=sqlContext.sql(sqlString26)
    record26.registerTempTable("ux3")

    val sqlString27="select a.* ,b.num as uxz4 from ux3 a left join (select userId,num from ua34XZweek where weekday='4') " +
      "b on a.userId=b.userId"
    val record27=sqlContext.sql(sqlString27)
    record27.registerTempTable("ux4")

    val sqlString28="select a.*,b.num as uxz5 from ux4 a left join (select userId,num from ua34XZweek where weekday='5')" +
      "b on a.userId=b.userId"
    val record28=sqlContext.sql(sqlString28)
    record28.registerTempTable("ux5")

    val sqlString29="select a.*,b.num as uxz6 from ux5 a left join (select userId,num from ua34XZweek where weekday='6')" +
      "b on a.userId=b.userId"
    val record29=sqlContext.sql(sqlString29)
    record29.registerTempTable("ux6")

    val sqlString30="select a.*,b.num as uxz7 from ux6  a left join (select userId,num from ua34XZweek where weekday='7')" +
      "b on a.userId=b.userId"
    val record30=sqlContext.sql(sqlString30)
    record30.registerTempTable("ux7")

    val sqlString31="select a.*,b.num as ubInva from ux7 a left join (select userId,num from ua34ACInva where actionType='1') b" +
      " on a.userId=b.userId"
    val record31=sqlContext.sql(sqlString31)
    record31.registerTempTable("ubInva")

    val sqlString32="select a.*,b.num as usInva from ubInva a left join (select userId,num from ua34ACInva where actionType='2')" +
      " b on a.userId=b.userId "
    val record32=sqlContext.sql(sqlString32)
    record32.registerTempTable("usInva")

    val sqlString33="select a.*,b.num as uxInva from usInva a left join (select userId,num from ua34ACInva where actionType='3')" +
      " b on a.userId=b.userId"
    val record33=sqlContext.sql(sqlString33)
    record33.registerTempTable("uxInva")

    //user309 buchong
    /*val sqlString34="select a.*,b.num as ubInva from user309all a left join (select userId,num from ua34ACInva where actionType='1')" +
      " b on a.userId=b.userId"
    val record34=sqlContext.sql(sqlString34)
    record34.registerTempTable("u309bInva")

    val sqlString35="select a.*,b.num as usInva from u309bInva a left join (select userId,num from ua34ACInva where actionType='2')" +
      " b on a.userId=b.userId"
    val record35=sqlContext.sql(sqlString35)
    record35.registerTempTable("u309sInva")

    val sqlString36="select a.*,b.num as uxInva from u309sInva a left join (select userId,num from ua34ACInva where actionType='3')" +
      " b on a.userId=b.userId"
    val record36=sqlContext.sql(sqlString36)
    record36.registerTempTable("u309xInva")*/

    /////songde tezheng kaishi
    val sqlString37="select songId ,actionType,weekday,count(*) as num from ua34JoinDay group by songId,actionType,weekday"
    val record37=sqlContext.sql(sqlString37)
    record37.registerTempTable("suaw")

    val sqlString38="select songId,actionType,count(*)as num from ua34JoinDay where isVa='1' group by songId,actionType"
    val record38=sqlContext.sql(sqlString38)
    record38.registerTempTable("suaInva")

    val sqlString39="select a.*,b.num as sbf1 from song56 a left join (select songId,num from suaw where actionType='1' and weekday='1')" +
      " b on a.songId=b.songId"
    val record39=sqlContext.sql(sqlString39)
    record39.registerTempTable("sb1")

    val sqlString40="select a.*,b.num as sbf2 from sb1 a left join (select songId,num from suaw where actionType='1' and weekday='2')" +
      " b on a.songId=b.songId"
    val record40=sqlContext.sql(sqlString40)
    record40.registerTempTable("sb2")

    val sqlString41="select a.*,b.num as sbf3 from sb2 a left join (select songId,num from suaw where actionType='1' and weekday='3')" +
      " b on a.songId=b.songId"
    val record41=sqlContext.sql(sqlString41)
    record41.registerTempTable("sb3")

    val sqlString42="select a.*,b.num as sbf4 from sb3 a left join (select songId,num from suaw where actionType='1' and weekday='4')" +
      " b on a.songId=b.songId"
    val record42=sqlContext.sql(sqlString42)
    record42.registerTempTable("sb4")

    val sqlString43 ="select a.*,b.num as sbf5 from sb4 a left join (select songId,num from suaw where actionType='1' and weekday='5')" +
      " b on a.songId=b.songId"
    val record43=sqlContext.sql(sqlString43)
    record43.registerTempTable("sb5")

    val sqlString44="select a.*,b.num as sbf6 from sb5 a left join (select songId,num from suaw where actionType='1' and weekday='6') " +
      " b on a.songId=b.songId"
    val record44=sqlContext.sql(sqlString44)
    record44.registerTempTable("sb6")

    val sqlString45="select a.*,b.num as sbf7 from sb6 a left join (select songId,num from suaw where actionType='1' and weekday='7')" +
      " b on a.songId=b.songId"
    val record45=sqlContext.sql(sqlString45)
    record45.registerTempTable("sb7")


    val sqlString46="select a.*,b.num as ssc1 from sb7 a left join (select songId,num from suaw where actionType='2' and weekday='1')" +
      " b on a.songId=b.songId"
    val record46=sqlContext.sql(sqlString46)
    record46.registerTempTable("ss1")

    val sqlString47="select a.*,b.num as ssc2 from ss1 a left join (select songId,num from suaw where actionType='2' and weekday='2')" +
      " b on a.songId=b.songId"
    val record47=sqlContext.sql(sqlString47)
    record47.registerTempTable("ss2")

    val sqlString48="select a.*,b.num as ssc3 from ss2 a left join (select songId,num from suaw where actionType='2' and weekday='3')" +
      " b on a.songId=b.songId"
    val record48=sqlContext.sql(sqlString48)
    record48.registerTempTable("ss3")

    val sqlString49="select a.*,b.num as ssc4 from ss3 a left join (select songId,num from suaw where actionType='2' and weekday='4')" +
      " b on a.songId=b.songId"
    val record49=sqlContext.sql(sqlString49)
    record49.registerTempTable("ss4")

    val sqlString50 ="select a.*,b.num as ssc5 from ss4 a left join (select songId,num from suaw where actionType='2' and weekday='5')" +
      " b on a.songId=b.songId"
    val record50=sqlContext.sql(sqlString50)
    record50.registerTempTable("ss5")

    val sqlString51="select a.*,b.num as ssc6 from ss5 a left join (select songId,num from suaw where actionType='2' and weekday='6') " +
      " b on a.songId=b.songId"
    val record51=sqlContext.sql(sqlString51)
    record51.registerTempTable("ss6")

    val sqlString52="select a.*,b.num as ssc7 from ss6 a left join (select songId,num from suaw where actionType='2' and weekday='7')" +
      " b on a.songId=b.songId"
    val record52=sqlContext.sql(sqlString52)
    record52.registerTempTable("ss7")

    val sqlString53="select a.*,b.num as sxz1 from ss7 a left join (select songId,num from suaw where actionType='3' and weekday='1')" +
      " b on a.songId=b.songId"
    val record53=sqlContext.sql(sqlString53)
    record53.registerTempTable("sx1")

    val sqlString54="select a.*,b.num as sxz2 from sx1 a left join (select songId,num from suaw where actionType='3' and weekday='2')" +
      " b on a.songId=b.songId"
    val record54=sqlContext.sql(sqlString54)
    record54.registerTempTable("sx2")

    val sqlString55="select a.*,b.num as sxz3 from sx2 a left join (select songId,num from suaw where actionType='3' and weekday='3')" +
      " b on a.songId=b.songId"
    val record55=sqlContext.sql(sqlString55)
    record55.registerTempTable("sx3")

    val sqlString56="select a.*,b.num as sxz4 from sx3 a left join (select songId,num from suaw where actionType='3' and weekday='4')" +
      " b on a.songId=b.songId"
    val record56=sqlContext.sql(sqlString56)
    record56.registerTempTable("sx4")

    val sqlString57 ="select a.*,b.num as sxz5 from sx4 a left join (select songId,num from suaw where actionType='3' and weekday='5')" +
      " b on a.songId=b.songId"
    val record57=sqlContext.sql(sqlString57)
    record57.registerTempTable("sx5")

    val sqlString58="select a.*,b.num as sxz6 from sx5 a left join (select songId,num from suaw where actionType='3' and weekday='6') " +
      " b on a.songId=b.songId"
    val record58=sqlContext.sql(sqlString58)
    record58.registerTempTable("sx6")

    val sqlString59="select a.*,b.num as sxz7 from sx6 a left join (select songId,num from suaw where actionType='3' and weekday='7')" +
      "b on a.songId=b.songId"
    val record59=sqlContext.sql(sqlString59)
    record59.registerTempTable("sx7")

    val sqlString60 ="select a.*,b.num as sbfInva from sx7 a left join (select songId,num from suaInva where actionType='1' ) b " +
      "on a.songId=b.songId"
    val record60=sqlContext.sql(sqlString60)
    record60.registerTempTable("sbfInva")

    val sqlString61="select a.*,b.num as sscInva from sbfInva a left join (select songId,num from suaInva where actionType='2' ) b " +
      "on a.songId=b.songId"
    val record61=sqlContext.sql(sqlString61)
    record61.registerTempTable("sscInva")

    val sqlString62="select a.*,b.num as sxzInva from sscInva a left join (select songId,num from suaInva where actionType='3' )b " +
      "on a.songId=b.songId"
    val record62=sqlContext.sql(sqlString62)
    record62.registerTempTable("sxzInva")

    val sqlString63="select a.*,b.IntId,b.publishTime,b.songInitPlays,b.language,b.gender from sxzInva a left join " +
      "songIntId b on a.songId=b.songId "
    val record63=sqlContext.sql(sqlString63)
    record63.registerTempTable("song56all")

    val sqlStringTemp="select distinct userId,songId from useraction34 "
    val recordTemp=sqlContext.sql(sqlStringTemp)
    recordTemp.registerTempTable("us34")

    val sqlStringUSB="select userId,songId,count(*) as num from useraction34 where actionType='1' group by userId,songId"
    val recordUSB=sqlContext.sql(sqlStringUSB)
    recordUSB.registerTempTable("USB34")

    val sqlStringUSS="select userId,songId,count(*) as num from useraction34 where actionType='2' group by userId,songId"
    val recordUSS=sqlContext.sql(sqlStringUSS)
    recordUSS.registerTempTable("USS34")

    val sqlStringUSX="select userId,songId,count(*) as num from useraction34 where actionType='3' group by userId,songId"
    val recordUSX=sqlContext.sql(sqlStringUSX)
    recordUSX.registerTempTable("USX34")


    ///////////////////*************************************************
    val sqlStringUS1 ="select a.*,b.num as usb from us34 a left join USB34 b on a.userId=b.userId and a.songId=b.songId"
    val recordUS1=sqlContext.sql(sqlStringUS1)
    recordUS1.registerTempTable("usb")

    val sqlStringUS2="select a.*,b.num as uss from usb a left join USS34 b on a.userId=b.userId and a.songId=b.songId"
    val recordUS2=sqlContext.sql(sqlStringUS2)
    recordUS2.registerTempTable("usbs")

    val sqlStringUS3="select a.*,b.num as usx from usbs a left join USX34 b on a.userId=b.userId and a.songId=b.songId"
    val recordUS3=sqlContext.sql(sqlStringUS3)
    recordUS3.registerTempTable("usbsx")





    //zhunbeishengcheng zhengyangben de jieguola  usdn56   uxInva  song56all  day56

    val sqlString64="select a.*,b.ubf1,b.ubf2,b.ubf3,b.ubf4,b.ubf5,b.ubf6,b.ubf7,b.usc1,b.usc2,b.usc3,b.usc4,b.usc5,b.usc6,b.usc7,b.uxz1,b.uxz2,b.uxz3,b.uxz4,b.uxz5,b.uxz6,b.uxz7,b.ubInva,b.usInva,b.uxInva" +
      " from usdn56 a left join uxInva b on a.userId=b.userId"
    val record64=sqlContext.sql(sqlString64)
    record64.registerTempTable("uu")

    val sqlString65="select a.*,b.IntId,b.publishTime,b.songInitPlays,b.language,b.gender,b.sbf1,b.sbf2,b.sbf3,b.sbf4,b.sbf5,b.sbf6,b.sbf7,b.ssc1,b.ssc2,b.ssc3,b.ssc4,b.ssc5,b.ssc6,b.ssc7,b.sxz1,b.sxz2,b.sxz3,b.sxz4,b.sxz5,b.sxz6,b.sxz7,b.sbfInva,b.sscInva,b.sxzInva " +
      " from uu a left join  song56all b on a.songId=b.songId"
    val record65=sqlContext.sql(sqlString65)
    record65.registerTempTable("uus")

    val sqlString66="select a.*,b.weekday,b.isVa  from uus a left join day56 b on a.ds=b.ds"
    val record66=sqlContext.sql(sqlString66)
    record66.registerTempTable("uusd")

    val sqlString67 ="select a.*,b.usb,b.uss,b.usx from uusd a left join usbsx b on a.userId=b.userId and a.songId=b.songId"
    val record67=sqlContext.sql(sqlString67)
    record67.registerTempTable("zyb")

    val sqlString68="select ds,songId,userId,isNull(num),weekday,isVa,IntId,publishTime,songInitPlays,language,gender,isNull(usb),isNull(uss),isNull(usx)," +
      "isNull(sbf1),isNull(ssc1),isNull(sxz1), isNull(sbf2),isNull(ssc2),isNull(sxz2),isNull(sbf3),isNull(ssc3),isNull(sxz3),isNull(sbf4),isNull(ssc4),isNull(sxz4)," +
      "isNull(sbf5),isNull(ssc5),isNull(sxz5),isNull(sbf6),isNull(ssc6),isNull(sxz6),isNull(sbf7),isNull(ssc7),isNull(sxz7), isNull(sbfInva),isNull(sscInva)," +
      "isNull(sxzInva),isNull(usb),isNull(uss),isNull(usx)," +
      "isNull(ubf1),isNull(usc1),isNull(uxz1), isNull(ubf2),isNull(usc2),isNull(uxz2),isNull(ubf3),isNull(usc3)," +
      "isNull(uxz3),isNull(ubf4),isNull(usc4),isNull(uxz4),isNull(ubf5),isNull(usc5),isNull(uxz5)," +
      "isNull(ubf6),isNull(usc6),isNull(uxz6),isNull(ubf7),isNull(usc7),isNull(uxz7), isNull(ubInva),isNull(usInva),isNull(uxInva)  from zyb "
    val record68=sqlContext.sql(sqlString68)
    //record68.registerTempTable("")




    ///jiexialaishi fuyangben   u309xInva   song103all day56   usdn56
    //b.IntId,b.publishTime,b.songInitPlays,b.language,b.gender,b.sbo1,b.sbo2,b.sbo3,b.sbo4,b.sbo5,b.sbo6,b.sbo7,b.ssc1,b.ssc2,b.ssc3,b.ssc4," +
    //"b.ssc5,b.ssc6,b.ssc7,b.sxz1,b.sxz2,b.sxz3,b.sxz4,b.sxz5,b.sxz6,b.sxz7,b.sbfInva,b.sscInva,b.sxzInva
    val sqlString69="select a.*,b.* from user309all a   join " +
      "song103all b "
    val record69=sqlContext.sql(sqlString69)
    record69.registerTempTable("u309s103")

    val sqlString70="select a.*,b.usb,b.uss,b.usx from u309s103  a left join usbsx b on a.userId=b.userId and a.songId=b.songId "
    val record70=sqlContext.sql(sqlString70)
    record70.registerTempTable("u309s103us")

    val sqlString71="select a.*,b.* from u309s103us a join  day56 b "
    val record71=sqlContext.sql(sqlString71)
    record71.registerTempTable("u309s103usd")

    val sqlString72="select a.*,b.num from u309s103usd a left join usdn56 b on a.userId=b.userId and a.songId=b.songId and a.ds=b.ds"
    val record72=sqlContext.sql(sqlString72)
    //record72.foreach(println(_))
    record72.registerTempTable("unprocessfyb")



    val sqlString73="select ds,songId,userId,isNull(num),weekday,isVa,IntId,publishTime,songInitPlays,language,gender,isNull(usb),isNull(uss),isNull(usx)," +
      "isNull(sbf1),isNull(ssc1),isNull(sxz1),isNull(sbf2),isNull(ssc2),isNull(sxz2),isNull(sbf3),isNull(ssc3),isNull(sxz3),isNull(sbf4),isNull(ssc4),isNull(sxz4)," +
      "isNull(sbf5),isNull(ssc5),isNull(sxz5),isNull(sbf6),isNull(ssc6),isNull(sxz6),isNull(sbf7),isNull(ssc7),isNull(sxz7), isNull(sbfInva),isNull(sscInva)," +
      "isNull(sxzInva),isNull(usb),isNull(uss),isNull(usx)," +
      "isNull(ubf1),isNull(usc1),isNull(uxz1),isNull(ubf2),isNull(usc2),isNull(uxz2),isNull(ubf3),isNull(usc3)," +
      "isNull(uxz3),isNull(ubf4),isNull(usc4),isNull(uxz4),isNull(ubf5),isNull(usc5),isNull(uxz5)," +
      "isNull(ubf6),isNull(usc6),isNull(uxz6),isNull(ubf7),isNull(usc7),isNull(uxz7),isNull(ubInva),isNull(usInva),isNull(uxInva) from unprocessfyb " +
      "where num!='null' "
    //val sqlString73="select ds,songId from unprocessfyb"
    val record73=sqlContext.sql(sqlString73)
   /* println("***********************************************************zyb**********************\n\n\n")
    record68.foreach(println(_))
    println("***********************************************************fyb**********************\n\n\n")
    record73.foreach(println(_))*/
 /*   record73.registerTempTable("temptt")
    val sqlString74=" select * from temptt  "
    val record74=sqlContext.sql(sqlString74)*/
    val writer2 = new PrintWriter(new File(args(13)))
    //  writer.write(records4.collect().length.toString)
    for(recordtemp<- record73.collect()) {
      var i = 0
      for (row <- recordtemp.toSeq) {
        if(i!=0) {
          writer2.write(",")
        }


        writer2.write(row.toString)

        i+=1
      }
      writer2.println()

    }

    writer2.close()


    val writer = new PrintWriter(new File(args(12)))
    //  writer.write(records4.collect().length.toString)
    for(recordtemp<- record68.collect()) {
      var i = 0
      for (row <- recordtemp.toSeq) {
        if(i!=0) {
          writer.write(",")
        }


        writer.write(row.toString)

        i+=1
      }
      writer.println()

    }

    writer.close()

  }

}

