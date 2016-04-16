package sparkApp

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by iceke on 16/1/21.
 */
object ScanQuery {
  def main(args: Array[String]){
    if(args.length<4){
      System.err.println("Usage of Parameters: inputPath outputPath X appName")
      System.exit(1)
    }
    val sparkConf = new SparkConf().setAppName(args(3)).setMaster("local")
    val sparkContext = new SparkContext(sparkConf)
    //val sparkContext = FlintWCContext.create(sparkConf)
    val lines = sparkContext.textFile(args(0))
    val finalWords = lines.map(s => {
      val parts = s.split("\\s+")
      (parts(0),parts(1).toInt)
    })


    val result = finalWords.filter(_._2>args(2).toInt)
    //val results = words.reduceByKey(_+_)
    result.saveAsTextFile(args(1))
  }

}
