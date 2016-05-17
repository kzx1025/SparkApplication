package sparkApp

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * Created by root on 15-9-7.
 */
object SparkWC {

  def main(args: Array[String]){
    if(args.length<2){
      System.err.println("Usage of Parameters: inputPath ouputPath")
      System.exit(1)
    }
    val sparkConf = new SparkConf().setAppName("WC").setMaster("local")
    val sparkContext = new SparkContext(sparkConf)
    //val sparkContext = FlintWCContext.create(sparkConf)
    val lines = sparkContext.textFile(args(0))
    val words = lines.flatMap(s => {
      val parts = s.split("\\s+")
      parts.map(Integer.parseInt(_)).map((_,1))
    })
    val results = words.reduceByKey(_+_)
    results.saveAsTextFile(args(1))
  }

}
