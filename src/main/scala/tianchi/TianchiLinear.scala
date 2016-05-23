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
  * Created by iceke on 16/5/23.
  */
object TianchiLinear {
  def main(args: Array[String]): Unit = {
    if (args.length < 5) {
      System.err.println("Usage of Parameters: master positiveData1 positve testData " +
        "model(1:LBFGS,2:SGD,3:DecisionTree) outputPath ")
      System.exit(1)
    }
    val sparkConf = new SparkConf()
      .setMaster(args(0))
      .setAppName("TianChi")
    //.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(sparkConf)
    val data1 = sc.textFile(args(1))
   // val data2 = sc.textFile(args(2))
    val data2 = null


    val dataA = data1

    val data5 = sc.textFile(args(3))


    val rawTestData = data5.map { line =>
      val parts = line.split(",").drop(3).map(_.toDouble)

      LabeledPoint(parts(0), Vectors.dense(parts.slice(0, parts.length)))
    }

    val realPlayData = data5.map { line =>
      val parts = line.split(",")
      parts(2).toDouble
    }

    val artistInfo = data5.map{ line =>
      val parts = line.split(",")
      (parts(0),parts(1))
    }


    val positiveData = dataA.map { line =>

      val parts = line.split(",").drop(2).map(_.toDouble)

      LabeledPoint(parts(0), Vectors.dense(parts.slice(1, parts.length)))

    }



    val allData = positiveData

    // val positiveDataNum = positiveData.count()
    // val negativeDataNum = negativeData.count()

    //标准正规化处理
    val scaler = new StandardScaler(withMean = true, withStd = true)
    val scaler2 = scaler.fit(allData.map(x => x.features))
    val scaler3 = scaler.fit(rawTestData.map(x => x.features))

    val finalData = allData.map(x => LabeledPoint(x.label, scaler2.transform(x.features)))

    val finalTestData = rawTestData.map(x => LabeledPoint(x.label, scaler3.transform(x.features)))


    val trainingData = finalData

    trainingData.collect().foreach(println)
    finalTestData.collect().foreach(println)

    //finalTestData.take(100).foreach(println)
    // testUserData.take(100).foreach(println)


    val choice = args(4).toInt

    val resultData = {
      if (choice == 1) {
        //线性回归
        val numIterations = 2000
        val stepSize = 0.01
        val model = LinearRegressionWithSGD.train(trainingData, numIterations, stepSize)
        // model.save(sc,args(9))
        finalTestData.map { point =>

          val prediction = model.predict(point.features)

          prediction

        }.zip(artistInfo)

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

        }.zip(artistInfo)

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

        }.zip(artistInfo)

      }
    }






    val evaluateData = resultData.asInstanceOf[RDD[(Double, (String, String))]].map { t =>
      (t._2._1, t._2._2, t._1)
    }.zip(realPlayData).map(t => ((t._1._2, t._1._1), (t._2, t._1._3)))

    evaluateData.saveAsTextFile(args(5))

    val days = evaluateData.map { t => (t._1._2, 1) }.reduceByKey(_ + _).count()

    //(歌手,日期,实际播放数,预测值)
    val tempValue = evaluateData.map { t => (t._1._1, Math.pow((t._2._2 - t._2._1) / t._2._1, 2.0)) }.reduceByKey(_ + _)
    val tempValue2 = evaluateData.map { t => (t._1._1, Math.pow((t._2._2 - t._2._1) / t._2._1, 2.0)) }
    tempValue2.collect().foreach(println)

    //(歌手 方差)
    val fangcha = tempValue.map(t => (t._1, Math.sqrt(t._2 / days)))
    fangcha.collect().foreach(println)

    //(歌手 权重)
    val artistWeight = evaluateData.map { t => (t._1._1, t._2._1) }.reduceByKey(_ + _).map(t => (t._1, Math.sqrt(t._2)))

    artistWeight.collect().foreach(println)
    val scores = artistWeight.zip(fangcha).filter(t => t._1._1==t._2._1).map(t =>(t._1._1,t._1._2*(1.0-t._2._2)))
      .map(t => (1,t._2)).reduceByKey(_+_)

    scores.collect().foreach(println)

  }


}
