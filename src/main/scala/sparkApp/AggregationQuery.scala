package sparkApp

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by iceke on 16/1/21.
 */
object AggregationQuery {

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
      (parts(0).substring(1,args(2).toInt),parts(2).toInt)
    })

    val result = finalWords.reduceByKey(_+_)

    //finalWords.map(_._1.substring(1,args(2).toInt)).
     // .map(word => (word,1)).groupByKey().map(t => (t._1, t._2.sum))
    //val results = words.reduceByKey(_+_)
    result.saveAsTextFile(args(1))
  }
}
