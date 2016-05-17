package tianchi

import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkContext, SparkConf}


/**
  * Created by iceke on 16/5/17.
  */
object TianchiLR {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("TianChi")
      //.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(sparkConf)
    val data1 = sc.textFile("/Users/iceke/lab/TianChi/result_positive.csv")
    val data2 = sc.textFile("/Users/iceke/lab/TianChi/result_negetive.csv")
    val data3 = sc.textFile("/Users/iceke/lab/TianChi/traincsv.txt")
    val data4 = sc.textFile("/Users/iceke/lab/TianChi/cvcsv.txt")


    val positiveData = data1.map { line =>

      val parts = line.split(",").map(_.toDouble)

      LabeledPoint(parts(0), Vectors.dense(parts.slice(1, parts.length)))

    }

    val negativeData = data2.map { line =>

      val parts = line.split(",").map(_.toDouble)

      LabeledPoint(parts(0), Vectors.dense(parts.slice(1, parts.length)))

    }

    val allData = positiveData union negativeData

    val positiveDataNum = positiveData.count()
    val negativeDataNum = negativeData.count()

    //标准正规化处理
    val scaler = new StandardScaler(withMean = true, withStd = true)
    val scaler2 = scaler.fit(allData.map(x => x.features))
    val finalData = allData.map(x => LabeledPoint(x.label, scaler2.transform(x.features)))

    val finalPositiveData = sc.parallelize(finalData.take(positiveDataNum.toInt))
    val finalNegativeData = sc.parallelize(finalData.collect().drop(positiveDataNum.toInt))
    println(finalPositiveData.count()+","+finalNegativeData.count())


    //负样本采样
    val samplePositiveData = sc.parallelize(finalPositiveData.takeSample(withReplacement = false,positiveDataNum.toInt,42))
    val sampleNegativeData = sc.parallelize(finalNegativeData.takeSample(withReplacement = false,positiveDataNum.toInt,42))



    val positiveSplits = samplePositiveData.randomSplit(Array(0.8, 0.2), seed = 11L)
    val negativeSplits = sampleNegativeData.randomSplit(Array(0.8, 0.2), seed = 11L)

    val trainingData = positiveSplits(0) union negativeSplits(0)
    val testData = positiveSplits(1) union negativeSplits(1)


    val model = new LogisticRegressionWithLBFGS().setNumClasses(2).run(trainingData)

    val labelAndPreds = testData.map { point =>

      val prediction = model.predict(point.features)

      (point.label, prediction)

    }

    val metrics = new MulticlassMetrics(labelAndPreds)
    val precision = metrics.precision
    val trainErr = labelAndPreds.filter(r=> r._1 != r._2).count.toDouble / testData.count
    println(trainErr+","+precision)


  }


}
