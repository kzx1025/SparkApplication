package sparkSql

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by iceke on 16/4/15.
  */
object OpenRoom {
  case class Customer(name: String, gender: String, ctfId: String, birthday: String, address: String)

  def main(args: Array[String]) {


    val sconf = new SparkConf()
      .setMaster("local")//h230是我的hostname，须自行修改
      .setAppName("OpenRoomInfo")//应用名称
      .set("spark.executor.memory", "1g")//应用执行时所用内存1g
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")//序列化
    val sc = new SparkContext(sconf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val customer = sc.textFile("*.csv").map(_.split(",")).filter(line => line.length > 7).map(a => Customer(a(0), a(5), a(4), a(6), a(7)))
    val customerSchema = sqlContext.createDataFrame(customer)
    customerSchema.registerTempTable("customer")
    def toInt(s: String):Int = {
      try {
        s.toInt
      } catch {
        case e:Exception => 9999
      }
    }
    def myfun(birthday: String) : String = {
      var rt = "未知"
      if (birthday.length == 8) {
        val md = toInt(birthday.substring(4))
        if (md >= 120 & md <= 219)
          rt = "水瓶座"
        else if (md >= 220 & md <= 320)
          rt = "双鱼座"
        else if (md >= 321 & md <= 420)
          rt = "白羊座"
        else if (md >= 421 & md <= 521)
          rt = "金牛座"
        else if (md >= 522 & md <= 621)
          rt = "双子座"
        else if (md >= 622 & md <= 722)
          rt = "巨蟹座"
        else if (md >= 723 & md <= 823)
          rt = "狮子座"
        else if (md >= 824 & md <= 923)
          rt = "处女座"
        else if (md >= 924 & md <= 1023)
          rt = "天秤座"
        else if (md >= 1024 & md <= 1122)
          rt = "天蝎座"
        else if (md >= 1123 & md <= 1222)
          rt = "射手座"
        else if ((md >= 1223 & md <= 1231) | (md >= 101 & md <= 119))
          rt = "摩蝎座"
        else
          rt = "未知"
      }
      rt
    }
    sqlContext.udf.register("constellation",  (x:String) => myfun(x))
    var result = sqlContext.sql("SELECT count(*) FROM customer")
    result.collect().foreach(println)

    result = sqlContext.sql("SELECT constellation(birthday), count(constellation(birthday)) FROM customer group by constellation(birthday)")
    result.collect().foreach(println)
  }


}
