package tianchi

import java.text.SimpleDateFormat
import java.util.{Date, Calendar}

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}


/**
  * Created by iceke on 16/5/25.
  */
object TianchiGenData {

  case class UserAction(userId: String, songId: String, gmtCreate: String, actionType: String, ds: String)

  case class SongIntId(IntId: String, songId: String, artistId: String, publishTime: String, songInitPlays: String,
                       language: String, gender: String)

  case class dayall(ds: String, weekday: String, isVa: String)

  case class adn(artistId: String, ds: String, num: String)

  def main(args: Array[String]): Unit = {
    if (args.length < 5) {
      System.err.println("Usage of Parameters: master user_actions.csv songsIntid.csv  day.csv  adn.csv  months  output ")
      System.exit(1)
    }
    val sparkConf = new SparkConf()
      .setAppName("TianChiGenData")
      .setMaster("local")
      //.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //.setMaster("local").set("spark.executor.memory","2g")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    val users = sc.textFile(args(1))
      .map(_.split("\\s+"))
      .filter(line => line.length == 5)
      .map(a => UserAction(a(0), a(1), a(2), a(3), a(4)))
    val userScheme = sqlContext.createDataFrame(users)
    userScheme.registerTempTable("tianchi_users")

    val songs = sc.textFile(args(2)).map(_.split(","))
      .filter(line => line.length == 7)
      .map(a => SongIntId(a(0), a(1), a(2), a(3), a(4), a(5), a(6)))
    val songScheme = sqlContext.createDataFrame(songs)
    songScheme.registerTempTable("songIntId")

    val day = sc.textFile(args(3)).map(_.split(","))
      .filter(line => line.length == 3)
      .map(a => dayall(a(0), a(1), a(2)))
    val dayallScheme = sqlContext.createDataFrame(day)
    dayallScheme.registerTempTable("dayall")


    val adnt = sc.textFile(args(4))
      .map(_.split(","))
      .filter(line => line.length == 3)
      .map(a => adn(a(0), a(1), a(2)))
    val adnScheme = sqlContext.createDataFrame(adnt)
    adnScheme.registerTempTable("adn")
    sqlContext.sql("select distinct artistId from adn").registerTempTable("artists")

    val cal: Calendar = Calendar.getInstance()
    // val vacationSet = Set("20150404", "20150405", "20150406", "20150501", "20150502", "20150503", "20150620", "20150621", "20150622")

    def isfml(time: String): String = {
      val year = time.substring(0, 4).toInt
      val month = time.substring(4, 6).toInt
      val day = time.substring(6, 8).toInt
      val a: String = new SimpleDateFormat("yyyyMMdd").format(new Date(year, month, day))

      val tempDate: Date = new SimpleDateFormat("yyyyMMdd").parse(a)

      cal.setTime(tempDate)
      if (cal.get(Calendar.DAY_OF_MONTH) < 11)
        "1"
      else if (cal.get(Calendar.DAY_OF_MONTH) < 21)
        "2"
      else
        "3"
    }

    sqlContext.udf.register("isfml", (time: String) => isfml(time))

    def preDay(weekday: String): String = {
      val i = weekday.toInt
      if (i == 1)
        "7"
      else
        (i - 1).toString
    }
    def nextDay(weekday: String): String = {
      val i = weekday.toInt
      if (i == 7)
        "1"
      else
        (i + 1).toString
    }
    sqlContext.udf.register("preDay", (weekday: String) => preDay(weekday))
    sqlContext.udf.register("nextDay", (weekday: String) => nextDay(weekday))

    def isDate(time: String): String = {
      val year = time.substring(0, 4).toInt
      val month = time.substring(4, 6).toInt
      val day = time.substring(6, 8).toInt
      val a: String = new SimpleDateFormat("yyyyMMdd").format(new Date(year, month, day))

      val tempDate: Date = new SimpleDateFormat("yyyyMMdd").parse(a)

      cal.setTime(tempDate)
      cal.get(Calendar.DAY_OF_MONTH).toString

    }

    sqlContext.udf.register("isDate", (time: String) => isDate(time))
    def isZero(action_num: String): String = {
      if (action_num == null) {
        "1"
      }
      else action_num
    }
    sqlContext.udf.register("isZero", (action_num: String) => isZero(action_num))


    def isNull(action_num: String): String = {
      if (action_num == null) {
        "0"
      }
      else action_num
    }
    sqlContext.udf.register("isNull", (action_num: String) => isNull(action_num))


    if(args(5) == "56"){
      sqlContext.sql("select * from dayall where ds<'20150701' and ds>'20150430' ").registerTempTable("subday")
      sqlContext.sql("select * from adn where ds<'20150701' and ds>'20150430' ").registerTempTable("subplay")
      sqlContext.sql("select a.*,b.weekday,b.isVa from subplay a left join subday b where a.ds=b.ds").registerTempTable("playday")
      sqlContext.sql("select * from adn where ds<'20150501' ").registerTempTable("preadn")

      sqlContext.sql("select from adn left join ")
    }




  }
}