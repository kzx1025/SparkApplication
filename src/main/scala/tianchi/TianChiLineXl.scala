package tianchi

import java.io.{File, PrintWriter}
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import tianchi.TianChiLineYc.{SongIntId, UserAction, dayall}

/**
  * Created by feiwang on 16-5-23.
  */
object TianChiLineXl {
  case class  adn(artistId:String,ds:String,num:String)

  def main(args:Array[String]): Unit = {
    if (args.length < 5) {
      System.err.println("Usage of Parameters: tianchi_user.csv songsIntid.csv  day.csv  disUs.csv   outputyb ")
      System.exit(1)
    }
    val sparkConf = new SparkConf()
      .setAppName("TianChifor78zyb")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //.setMaster("local").set("spark.executor.memory","2g")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    val users = sc.textFile(args(0))
      .map(_.split("\\s+"))
      .filter(line => line.length == 5)
      .map(a => UserAction(a(0), a(1), a(2), a(3), a(4)))
    val userScheme = sqlContext.createDataFrame(users)
    userScheme.registerTempTable("tianchi_users")

    val songs = sc.textFile(args(1)).map(_.split(","))
      .filter(line => line.length == 7)
      .map(a => SongIntId(a(0), a(1), a(2), a(3), a(4), a(5), a(6)))
    val songScheme = sqlContext.createDataFrame(songs)
    songScheme.registerTempTable("songIntId")

    val day = sc.textFile(args(2)).map(_.split(","))
      .filter(line => line.length == 3)
      .map(a => dayall(a(0), a(1), a(2)))
    val dayallScheme = sqlContext.createDataFrame(day)
    dayallScheme.registerTempTable("dayall")


    val adnt = sc.textFile(args(3))
      .map(_.split(","))
      .filter(line => line.length == 3)
      .map(a => adn(a(0), a(1), a(2)))
    val adnScheme = sqlContext.createDataFrame(adnt)
    adnScheme.registerTempTable("adn")
    sqlContext.sql("select distinct artistId from adn ").registerTempTable("artists")

    val cal: Calendar = Calendar.getInstance()
   // val vacationSet = Set("20150404", "20150405", "20150406", "20150501", "20150502", "20150503", "20150620", "20150621", "20150622")

    def isfml(time: String): String = {
      val year = time.substring(0,4).toInt
      val month=time.substring(4,6).toInt
      val day=time.substring(6,8).toInt
      val a: String = new SimpleDateFormat("yyyyMMdd").format(new Date(year,month,day))

      val tempDate: Date = new SimpleDateFormat("yyyyMMdd").parse(a)

      cal.setTime(tempDate)
      if(cal.get(Calendar.DAY_OF_MONTH)<11)
        "1"
      else  if(cal.get(Calendar.DAY_OF_MONTH)<21)
        "2"
      else
        "3"
    }

    sqlContext.udf.register("isfml",(time:String)=>isfml(time))

    def preDay(weekday:String):String={
      val i=weekday.toInt
      if(i==1)
        "7"
      else
        (i-1).toString
    }
    def nextDay(weekday:String):String={
      val i=weekday.toInt
      if(i==7)
        "1"
      else
        (i+1).toString
    }
    sqlContext.udf.register("preDay",(weekday:String)=>preDay(weekday))
    sqlContext.udf.register("nextDay",(weekday:String)=>nextDay(weekday))

    def isDate(time: String): String = {
      val year = time.substring(0,4).toInt
      val month=time.substring(4,6).toInt
      val day=time.substring(6,8).toInt
      val a: String = new SimpleDateFormat("yyyyMMdd").format(new Date(year,month,day))

      val tempDate: Date = new SimpleDateFormat("yyyyMMdd").parse(a)

      cal.setTime(tempDate)
      cal.get(Calendar.DAY_OF_MONTH).toString

    }

    sqlContext.udf.register("isDate",(time:String)=>isDate(time))
    def isZero(action_num :String):String={
      if (action_num == null){
        "1"
      }
      else action_num
    }
    sqlContext.udf.register("isZero",(action_num:String)=> isZero(action_num))


    def isNull(action_num :String):String={
      if (action_num == null){
        "0"
      }
      else action_num
    }
    sqlContext.udf.register("isNull",(action_num:String)=> isNull(action_num))

    if(args(4)=="56"){
      sqlContext.sql("select * from dayall where ds<'20150701' and ds>'20150430' ").registerTempTable("dayn")
      sqlContext.sql("select * from tianchi_users where ds<'20150501' ").registerTempTable("usa")
      sqlContext.sql("select * from tianchi_users where ds>'20150430' and ds<'20150701' ").registerTempTable("temp")
      sqlContext.sql("select a.*,b.artistId from temp a join songIntId b on a.songId=b.songId").registerTempTable("temp2")
     // sqlContext.sql("select artistId,ds,count(*) as num from temp2 where actionType='1' group by artistId,ds").registerTempTable("adn")

      sqlContext.sql("select artistId,count(*) as num from songIntId where publishTime>'20150228' and publishTime<'20150501' group by artistId")
        .registerTempTable("ansn")
      sqlContext.sql("select * from adn where ds<'20150701' and ds>'20150430'").registerTempTable("adnn")
      sqlContext.sql("select * from adn where ds<'20150501' ").registerTempTable("adnq")

    }
    if(args(4)=="78"){
      sqlContext.sql("select * from dayall where ds<'20150901' and ds>'20150630' ").registerTempTable("dayn")
      sqlContext.sql("select * from tianchi_users where ds<'20150701' and ds>'20150430' ").registerTempTable("usa")
      sqlContext.sql("select * from tianchi_users where ds>'20150630' and ds<'20150831' ").registerTempTable("temp")
      sqlContext.sql("select a.*,b.artistId from temp a join songIntId b on a.songId=b.songId").registerTempTable("temp2")
     // sqlContext.sql("select artistId,ds,count(*) as num from temp2 where actionType='1' group by artistId,ds").registerTempTable("adn")

      sqlContext.sql("select artistId,count(*) as num from songIntId where publishTime>'20150430' and publishTime<'20150701' group by artistId")
        .registerTempTable("ansn")
      sqlContext.sql("select * from adn where ds<'20150831' and ds>'20150630'").registerTempTable("adnn")
      sqlContext.sql("select * from adn where ds<'20150701' and ds>'20150430' ").registerTempTable("adnq")

    }
    sqlContext.sql("select a.*,b.weekday,b.isVa from usa a join dayall  b on a.ds=b.ds").registerTempTable("usad")
    sqlContext.sql("select a.*,b.artistId from usad a join songIntId b on a.songId=b.songId")
      .registerTempTable("usadart")

    sqlContext.sql("select a.*,b.weekday,b.isVa,isDate(a.ds) as dn " +
      " from adnq a left join dayall b on a.ds=b.ds").registerTempTable("t0")
    println(sqlContext.sql("select * from t0 "))
    println("thisis")
    ////////// bf
    sqlContext.sql("select artistId,weekday,sum(num) as num from t0 group by artistId,weekday").registerTempTable("t1")
    sqlContext.sql("select artistId,isVa,sum(num) as num from t0 group by artistId,isVa").registerTempTable("t2")
    sqlContext.sql("select artistId,dn,avg(num) as num from t0 group by artistId,dn").registerTempTable("t100")
    /////////////sc xz
    sqlContext.sql("select artistId,weekday,count(*) as num  from usadart where actionType='2' group by artistId,weekday").registerTempTable("t3")
    sqlContext.sql("select artistId,weekday,count(*) as num from usadart where actionType='3' group by artistId,weekday").registerTempTable("t4")
    sqlContext.sql("select artistId,isVa,count(*) as num from usadart where actionType='2' group by artistId,isVa").registerTempTable("t5")
    sqlContext.sql("select artistId,isVa,count(*) as num from usadart where actionType='3' group by artistId,isVa").registerTempTable("t6")

    //////////////////ag
    sqlContext.sql("select distinct artistId,gender from songIntId ").registerTempTable("ag")
    ////////

    sqlContext.sql("select a.*,b.weekday,b.isVa,isDate(b.ds) as dn ,preDay(b.weekday) as pd ,nextDay(b.weekday) as nd ,isfml(b.ds) as fml from" +
      " adnn a left join dayall b on a.ds=b.ds").registerTempTable("tt1")
    sqlContext.sql("select a.*,b.gender from tt1 a left join ag b on a.artistId=b.artistId").registerTempTable("tt2")
    //sw
    sqlContext.sql("select a.*,b.num as awb from tt2 a left join t1 b on a.artistId=b.artistId and a.weekday=b.weekday").registerTempTable("tt3")
    sqlContext.sql("select a.*,b.num as aws from tt3 a left join t3 b on a.artistId=b.artistId and a.weekday=b.weekday").registerTempTable("tt4")
    sqlContext.sql("select a.*,b.num as awx from tt4 a left join t4 b on a.artistId=b.artistId and a.weekday=b.weekday").registerTempTable("tt5")
    //vs
    sqlContext.sql("select a.*,b.num as bInva from tt5 a left join t2 b on a.artistId=b.artistId and a.isVa=b.isVa").registerTempTable("tt6")
    sqlContext.sql("select a.*,b.num as sInva from tt6 a left join t5 b on a.artistId=b.artistId and a.isVa=b.isVa").registerTempTable("tt7")
    sqlContext.sql("select a.*,b.num as xInva from tt7 a left join t6 b on a.artistId=b.artistId and a.isVa=b.isVa").registerTempTable("tt8")

    ///ns
    sqlContext.sql("select a.*,b.num as nsn from tt8 a left join ansn b on a.artistId=b.artistId").registerTempTable("tt9")
    sqlContext.sql("select a.*,b.num as pdb from tt9 a left join t1 b on a.artistId=b.artistId and a.pd=b.weekday ").registerTempTable("tt10")
    sqlContext.sql("select a.*,b.num as pds from tt10 a left join t3 b on a.artistId=b.artistId and a.pd=b.weekday ").registerTempTable("tt11")

    sqlContext.sql("select a.*,b.num as pdx from tt11 a left join t4 b on a.artistId=b.artistId and a.pd=b.weekday ").registerTempTable("tt12")

    sqlContext.sql("select a.*,b.num as ndb from tt12 a left join t1 b on a.artistId=b.artistId and a.nd=b.weekday").registerTempTable("tt13")
    sqlContext.sql("select a.*,b.num as nds from tt13 a left join t3 b on a.artistId=b.artistId and a.nd=b.weekday").registerTempTable("tt14")
    sqlContext.sql("select a.*,b.num as ndx from tt14 a left join t4 b on a.artistId=b.artistId and a.nd=b.weekday").registerTempTable("tt15")
    /////////
    println(sqlContext.sql("select * from t100"))
    println("thisis")
    sqlContext.sql("select a.*,c.num as swb from tt15 a left join t100 c on a.artistId=c.artistId and a.dn=c.dn ").registerTempTable("tt16")















    val yb=sqlContext.sql("select ds,artistId,isZero(num),weekday,isVa,gender,isNull(awb),isNull(aws),isNull(awx)," +
      "isNull(bInva),isNull(sInva),isNull(xInva),isNull(nsn),fml,isNull(swb),isNull(pdb),isNull(pds),isNull(pdx),isNull(ndb),isNull(nds)," +
      "isNull(ndx) from tt16")
    yb.map(t=>{
      var r=""
      for(i<-0 until t.length-1){
        r=r+t(i)+","
      }
      r=r+t(t.length-1)
      r
    }).saveAsTextFile(args(5))
//val writer = new PrintWriter(new File(args(4)))
//    //  writer.write(records4.collect().length.toString)
//    for(recordtemp<- yb.collect()) {
//      var i = 0
//      for (row <- recordtemp.toSeq) {
//        if(i!=0) {
//          writer.write(",")
//        }
//
//
//        writer.write(row.toString)
//
//        i+=1
//      }
//      writer.println()
//
//    }
//
//    writer.close()





  }

}
