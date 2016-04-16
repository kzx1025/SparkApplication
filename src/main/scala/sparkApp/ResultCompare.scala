package sparkApp


import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by root on 15-9-8.
 */
object ResultCompare {

  def main(args: Array[String]){
    val sparkConf = new SparkConf().setAppName("ResultCompare")
    val sparkContext = new SparkContext(sparkConf)
    val linesA = sparkContext.textFile(args(0))
    val linesB = sparkContext.textFile(args(1))
    val testData = Array[String]("26297630","44440715","37028454","7197823","45920739")

    def judge(line:String):Boolean={
      for(i<- 0 until 5){
        if(line.contains(testData(i)))
          return true
      }
      return false
    }

    val valueA = linesA.filter(f => judge(f)).toArray()
    val valueB = linesB.filter(f => judge(f)).toArray()
    println("1."+valueA.mkString(";"))
    println("2."+valueB.mkString(";"))

  }

}
