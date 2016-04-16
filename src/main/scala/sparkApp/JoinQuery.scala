package sparkApp

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by iceke on 16/1/21.
 */
object JoinQuery {
  def main(args: Array[String]){
    if(args.length<4){
      System.err.println("Usage of Parameters: inputPath1 inputPath2 outputPath appName")
      System.exit(1)
    }
    val sparkConf = new SparkConf().setAppName(args(3)).setMaster("local")
    val sparkContext = new SparkContext(sparkConf)
    //val sparkContext = FlintWCContext.create(sparkConf)
    val lines1 = sparkContext.textFile(args(0))
    val lines2 = sparkContext.textFile(args(1))
    val finalWords1 = lines1.flatMap(s => {
      val parts = s.split("\\s+")
      parts
    }).map(word => (word,1))

    val finalWords2 = lines2.flatMap(s => {
      val parts = s.split("\\s+")
      parts
    }).map(word => (word,2))

    val finalWords = finalWords1.join(finalWords2)
    //val results = words.reduceByKey(_+_)
    finalWords.saveAsTextFile(args(2))
  }

}
