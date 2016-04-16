package sparkApp

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by root on 15-9-9.
 */
object SparkPRDataInit {
  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("SparkPRDataInit")
    val ctx = new SparkContext(sparkConf)
    val lines = ctx.textFile(args(0))
    val links = lines.map { s =>
      val parts = s.split("\\s+")
      parts(1) + " " + parts(2)
    }
    links.saveAsTextFile(args(1))
  }
}
