package sparkSql

import java.io.PrintWriter

import java.io._
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by iceke on 16/4/16.
  */
object TianChi {
  case class User(userId: String, songId: String, gmtCreate: String, actionType: String, ds: String)

  def main(args: Array[String]): Unit ={
    if(args.length<2){
      System.err.println("Usage of Parameters: master inputPath outputPath")
      System.exit(1)
    }
    val sparkConf = new SparkConf()
      .setMaster(args(0))
      .setAppName("TianChi")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    val user = sc.textFile(args(1)).map(_.split(","))
      .filter(line => line.length == 5)
      .map(a => User(a(0),a(1),a(2),a(3),a(4)))
    val userSchema = sqlContext.createDataFrame(user)
    userSchema.registerTempTable("user")

    def toInt(s:String): Int={
      try{
        s.toInt
      } catch {
        case e:Exception => 9999
      }
    }

    val sqlString = "SELECT songId, count(songId) FROM user WHERE actionType='1' group by songId"
    //val sqlString2 = "SELECT * FROM user"
    val result = sqlContext.sql(sqlString)
    result.collect().foreach(println)
    val writer = new PrintWriter(new File(args(2)))
    for(a <- result.collect()){
      writer.write(a.toString())
      writer.println()
    }
    writer.close()

  }

}
