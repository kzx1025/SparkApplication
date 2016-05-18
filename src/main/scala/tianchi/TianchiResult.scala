package tianchi

import java.io.{File, PrintWriter}

import org.apache.spark.mllib.classification.{LogisticRegressionWithSGD, LogisticRegressionWithLBFGS}
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.optimization.L1Updater
import org.apache.spark.mllib.regression.{GeneralizedLinearModel, LinearRegressionWithSGD, LabeledPoint}
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}


/**
  * Created by iceke on 16/5/17.
  */
object TianchiResult {

  def main(args: Array[String]): Unit = {
    if (args.length < 5) {
      System.err.println("Usage of Parameters: master positiveData negativeData testData model(1:LBFGS,2:SGD,3:DecisionTree) outputPath")
      System.exit(1)
    }
    val sparkConf = new SparkConf()
      .setMaster(args(0))
      .setAppName("TianChi")
    //.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(sparkConf)
    val data1 = sc.textFile(args(1))
    val data2 = sc.textFile(args(2))
    val data3 = sc.textFile(args(3))


    val rawTestData = data3.map{line =>
      val parts = line.split(",").drop(3).map(_.toDouble)

      LabeledPoint(parts(0), Vectors.dense(parts.slice(1, parts.length)))
    }

    val testUserData = data3.map{line =>
      val parts = line.split(",")
      (parts(0),parts(1),parts(2))
    }


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
    val scaler3 = scaler.fit(rawTestData.map(x => x.features))

    val finalData = allData.map(x => LabeledPoint(x.label, scaler2.transform(x.features)))

    val finalTestData = rawTestData.map(x => LabeledPoint(x.label, scaler3.transform(x.features)))

    val finalPositiveData = sc.parallelize(finalData.take(positiveDataNum.toInt))
    val finalNegativeData = sc.parallelize(finalData.collect().drop(positiveDataNum.toInt))
    println(finalPositiveData.count() + "," + finalNegativeData.count())


    //正负样本采样
    val samplePositiveData = sc.parallelize(finalPositiveData.takeSample(withReplacement = false, positiveDataNum.toInt, 42))
    val sampleNegativeData = sc.parallelize(finalNegativeData.takeSample(withReplacement = false, positiveDataNum.toInt, 42))



    val trainingData = samplePositiveData union sampleNegativeData




    val choice = args(4).toInt

    val resultData = {
      if (choice == 1) {
        //逻辑回归 最大似然估计
        val model = new LogisticRegressionWithLBFGS().setNumClasses(2).run(trainingData)
        finalTestData.map { point =>

          val prediction = model.predict(point.features)

          prediction

        }.zip(testUserData)

      } else if (choice == 2) {
        //逻辑回归 梯度下降法
        val numIterations = 20000
        val stepSize = 0.00000001
        val lrWithSGD = new LogisticRegressionWithSGD()
        lrWithSGD.optimizer.setNumIterations(numIterations)
          .setStepSize(stepSize).setUpdater(new L1Updater())
        val model = lrWithSGD.run(trainingData)

        finalTestData.map { point =>

          val prediction = model.predict(point.features)

           prediction

        }.zip(testUserData)

      } else if (choice == 3) {
        val numClasses = 2
        val categoricalFeaturesInfo = Map[Int, Int]()
        val impurity = "gini"
        val maxDepth = 5
        val maxBins = 32

        //决策树
        val model = DecisionTree.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
          impurity, maxDepth, maxBins)


        finalTestData.map { point =>

          val prediction = model.predict(point.features)
          prediction

        }.zip(testUserData)

      }
    }


    val writer = new PrintWriter(new File(args(5)))
    for(record <- resultData.asInstanceOf[RDD[_]].collect()) {

      val tempRecord = record.asInstanceOf[(Double,(String,String,String))]
      writer.write(tempRecord._1+","+tempRecord._2._1+","+tempRecord._2._2+","+tempRecord._2._3)
      writer.println()

    }

    writer.close()


  }


}
