package tianchi

import java.io.{File, PrintWriter}

import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{RidgeRegressionWithSGD, LassoWithSGD, LinearRegressionWithSGD, LabeledPoint}
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.mllib.tree.{GradientBoostedTrees, RandomForest, DecisionTree}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by iceke on 16/5/24.
  */
object TianchiLinearResult {
  def main(args: Array[String]): Unit = {
    if (args.length < 5) {
      System.err.println("Usage of Parameters: master positiveData1  positveData2 testData " +
        "model(1:LBFGS,2:SGD,3:DecisionTree) outputPath ")
      System.exit(1)
    }
    val sparkConf = new SparkConf()
      .setMaster(args(0))
      .setAppName("TianChi")
    //.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(sparkConf)
    val data1 = sc.textFile(args(1))
     val data2 = sc.textFile(args(2))
     // val data2 = null


    val dataA = data1 union data2

    val data5 = sc.textFile(args(3))


    val rawTestData = data5.map { line =>
      val parts = line.split(",").drop(2).map(_.toDouble)

      LabeledPoint(parts(0), Vectors.dense(parts.slice(0, parts.length-2)))
    }


    val artistInfo = data5.map { line =>
      val parts = line.split(",")
      (parts(0), parts(1))
    }


    val positiveData = dataA.map { line =>

      val parts = line.split(",").drop(2).map(_.toDouble)

      LabeledPoint(parts(0), Vectors.dense(parts.slice(1, parts.length-2)))

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
        //线性回归1
        val numIterations = 2000
        val stepSize = 0.00000001
        val model = LinearRegressionWithSGD.train(trainingData, numIterations)
        // model.save(sc,args(9))
        finalTestData.map { point =>

          val prediction = model.predict(point.features)

          prediction.toInt

        }.zip(artistInfo)

      } else if (choice == 2) {
        //线性回归2
        val numIterations = 2000
        val stepSize = 0.1
        val model = LassoWithSGD.train(trainingData, numIterations)


        finalTestData.map { point =>

          val prediction = model.predict(point.features)

          prediction.toInt

        }.zip(artistInfo)

      } else if (choice == 3) {
        //线性回归3
        val numIterations = 2000
        val stepSize = 0.1
        val model = RidgeRegressionWithSGD.train(trainingData, numIterations)

        finalTestData.map { point =>

          val prediction = model.predict(point.features)
          prediction.toInt

        }.zip(artistInfo)

      }
      else if (choice == 4) {
        //决策树
        val categoricalFeaturesInfo = Map[Int, Int]()
        val impurity = "variance"
        val maxDepth = 10
        val maxBins = 64

        val model = DecisionTree.trainRegressor(trainingData, categoricalFeaturesInfo, impurity,

          maxDepth, maxBins)

        finalTestData.map { point =>

          val prediction = model.predict(point.features)
          prediction.toInt

        }.zip(artistInfo)

      } else if (choice == 5) {
        //随机森林
        val categoricalFeaturesInfo = Map[Int, Int]()
        val numTrees = 4 // Use more in practice.
        val featureSubsetStrategy = "auto" // Let the algorithm choose.
        val impurity = "variance"
        val maxDepth = 8
        val maxBins = 50
        val model = RandomForest.trainRegressor(trainingData, categoricalFeaturesInfo,
          numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)

        finalTestData.map { point =>

          val prediction = model.predict(point.features)
          prediction.toInt

        }.zip(artistInfo)

      }
      else if (choice == 6) {
        //梯度提升树
        val boostingStrategy = BoostingStrategy.defaultParams("Regression")
        boostingStrategy.setNumIterations(50)
        boostingStrategy.getTreeStrategy.setMaxDepth(20)
        //boostingStrategy.getTreeStrategy.setCategoricalFeaturesInfo(Map[Int, Int]())

        val model = GradientBoostedTrees.train(trainingData, boostingStrategy)

        finalTestData.map { point =>

          val prediction = model.predict(point.features)
          prediction.toInt

        }.zip(artistInfo)

      }

    }



    val evaluateData = resultData.asInstanceOf[RDD[(Int, (String, String))]]
      .map(t => t._2._2+","+ t._1+","+t._2._1 )

    val writer = new PrintWriter(new File(args(5)))
    for(record <- evaluateData.collect()){
      writer.write(record)
      writer.println()
    }

    writer.close()





  }

}
