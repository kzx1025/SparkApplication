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
  * Created by iceke on 16/5/26.
  */
object LocalResult {
  def main(args: Array[String]): Unit = {
    if (args.length < 5) {
      System.err.println("Usage of Parameters: master trainData testData " +
        "model(1:LBFGS,2:SGD,3:DecisionTree) feature_out outputPath ")
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
      val parts0 = line.split(",")
      val parts = line.split(",").drop(2).map(_.toDouble)
      val parts2 = line.split(",").drop(3).map(_.toDouble)

      var add: List[Double] = List()
      for (part1 <- parts2) {
        for (part2 <- parts2) {
          add = add.:+(part1 * part2)
        }
      }
      println(add.length)

      val features: Array[Double] = parts.slice(0, parts.length - args(5).toInt)

      (parts0(0), parts0(1), LabeledPoint(parts(0), Vectors.dense(features)))
      //LabeledPoint(parts(0), Vectors.dense(parts.slice(1,args(5).toInt-1)++parts.slice(args(5).toInt+1,parts.length)))
    }




    val positiveData = dataA.map { line =>
      val parts0 = line.split(",")
      val parts = line.split(",").drop(2).map(_.toDouble)
      val parts2 = line.split(",").drop(3).map(_.toDouble)

      var add: List[Double] = List()
      for (part1 <- parts2) {
        for (part2 <- parts2) {
          add = add.:+(part1 * part2)
        }
      }
      println(add.length)

      val features: Array[Double] = parts.slice(1, parts.length - args(5).toInt)

      (parts0(0), parts0(1), LabeledPoint(parts(0), Vectors.dense(features)))
      // LabeledPoint(parts(0), Vectors.dense(parts.slice(1,args(5).toInt-1)++parts.slice(args(5).toInt+1,parts.length)))

    }

    val trainingNum = positiveData.count()

    val wholeData = positiveData union rawTestData


    //标准正规化处理
    val scaler = new StandardScaler(withMean = true, withStd = true)

    val scaler2 = scaler.fit(wholeData.map(x => x._3.features))
    val zhengguiData = wholeData.map(x => (x._1, x._2, LabeledPoint(x._3.label, scaler2.transform(x._3.features))))

    val trainingData = sc.parallelize(zhengguiData.take(trainingNum.toInt))

    val testData = sc.parallelize(zhengguiData.collect().drop(trainingNum.toInt))

    val finalTrainingData = trainingData.map(t => t._3)
    val finalTestData = testData.map(t => t._3)

    val artistInfo = testData.map(t => (t._1, t._2))

    //finalTestData.take(100).foreach(println)
    // testUserData.take(100).foreach(println)


    val choice = args(4).toInt

    val resultData = {
      if (choice == 1) {
        //线性回归1
        val numIterations = 2000
        val stepSize = 0.00000001
        val model = LinearRegressionWithSGD.train(finalTrainingData, numIterations)
        // model.save(sc,args(9))
        finalTestData.map { point =>

          val prediction = model.predict(point.features)

          prediction.toInt

        }.zip(artistInfo)

      } else if (choice == 2) {
        //线性回归2
        val numIterations = 2000
        val stepSize = 0.1
        val model = LassoWithSGD.train(finalTrainingData, numIterations)


        finalTestData.map { point =>

          val prediction = model.predict(point.features)

          prediction.toInt

        }.zip(artistInfo)

      } else if (choice == 3) {
        //线性回归3
        val numIterations = 2000
        val stepSize = 0.1
        val model = RidgeRegressionWithSGD.train(finalTrainingData, numIterations)

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

        val model = DecisionTree.trainRegressor(finalTrainingData, categoricalFeaturesInfo, impurity,

          maxDepth, maxBins)

        finalTestData.map { point =>

          val prediction = model.predict(point.features)
          prediction.toInt

        }.zip(artistInfo)

      } else if (choice == 5) {
        //随机森林
        val categoricalFeaturesInfo = Map[Int, Int]()
        val numTrees = 40 // Use more in practice.
        val featureSubsetStrategy = "auto" // Let the algorithm choose.
        val impurity = "variance"
        val maxDepth = 8
        val maxBins = 50
        val model = RandomForest.trainRegressor(finalTrainingData, categoricalFeaturesInfo,
          numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)

        finalTestData.map { point =>

          val prediction = model.predict(point.features)
          prediction.toInt

        }.zip(artistInfo)

      }
      else if (choice == 6) {
        //梯度提升树
        val boostingStrategy = BoostingStrategy.defaultParams("Regression")
        boostingStrategy.setNumIterations(88)
        boostingStrategy.getTreeStrategy.setMaxDepth(20)
        //boostingStrategy.getTreeStrategy.setCategoricalFeaturesInfo(Map[Int, Int]())

        val model = GradientBoostedTrees.train(finalTrainingData, boostingStrategy)

        finalTestData.map { point =>

          val prediction = model.predict(point.features)
          prediction.toInt

        }.zip(artistInfo)

      }

    }



    val evaluateData = resultData.asInstanceOf[RDD[(Int, (String, String))]]
      .map(t => t._2._2 + "," + t._1 + "," + t._2._1)

    val writer = new PrintWriter(new File(args(6)))
    for (record <- evaluateData.collect()) {
      writer.write(record)
      writer.println()
    }

    writer.close()


  }
}
