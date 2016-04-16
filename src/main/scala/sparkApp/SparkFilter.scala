package sparkApp

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by iceke on 15/9/25.
 */
object SparkFilter {
  def main(args: Array[String]){
  if(args.length<4){
    System.err.println("Usage of Parameters: inputPath outputPath filter appName")
    System.exit(1)
  }
  val sparkConf = new SparkConf().setAppName(args(3))
  val sparkContext = new SparkContext(sparkConf)
  //val sparkContext = FlintWCContext.create(sparkConf)
  val lines = sparkContext.textFile(args(0))
  val finalWords = lines.flatMap(s => {
    val parts = s.split("\\s+")
    parts
  }).filter(_.endsWith(args(2)))
  //val results = words.reduceByKey(_+_)
  finalWords.saveAsTextFile(args(1))
}

}
