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
object LocalTest {
  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      System.err.println("Usage of Parameters: master data " +
        "model(1:LBFGS,2:SGD,3:DecisionTree)feature_out outputPath fangcha")
      System.exit(1)
    }
    val sparkConf = new SparkConf()
      .setMaster(args(0))
      .setAppName("TianChi")
    //.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(sparkConf)
    val data = sc.textFile(args(1))
    // val data2 = sc.textFile(args(2))



    val allData = data.map{line =>
      val parts0 = line.split(",")
      val parts = line.split(",").drop(2).map(_.toDouble)

      val features:Array[Double] = parts.slice(1, parts.length)

      (parts0(0),parts0(1),LabeledPoint(parts(0), Vectors.dense(features)))
    }



    val allDataNum = allData.count()


    //标准正规化处理
    val scaler = new StandardScaler(withMean = true, withStd = true)

    val scaler2 = scaler.fit(allData.map(x => x._3.features))
    val zhengguiData = allData.map(x =>(x._1,x._2,LabeledPoint(x._3.label, scaler2.transform(x._3.features))))

    val trainingData = sc.parallelize(zhengguiData.take(allDataNum.toInt-1300))

    val testData = sc.parallelize(zhengguiData.collect().drop(allDataNum.toInt-1300))

    val finalTrainingData = trainingData.map(t=>t._3)
    val finalTestData = testData.map(t=> t._3)

    val artistInfo = testData.map(t=>(t._1,t._2))








    // val scaler2 = scaler.fit(allData.map(x => x.features))
    //val scaler3 = scaler.fit(rawTestData.map(x => x.features))

    //val finalData = allData.map(x => LabeledPoint(x.label, scaler2.transform(x.features)))

    // val finalTestData = rawTestData.map(x => LabeledPoint(x.label, scaler3.transform(x.features)))


    //finalTestData.take(100).foreach(println)
    // testUserData.take(100).foreach(println)


    val choice = args(2).toInt

    val resultData = {
      if (choice == 1) {
        //线性回归1
        val numIterations = 2000
        val stepSize = 0.00000001
        val model = LinearRegressionWithSGD.train(finalTrainingData, numIterations)
        // model.save(sc,args(9))
        finalTestData.map { point =>

          val prediction = model.predict(point.features)

          (prediction.toInt, point.label)

        }.zip(artistInfo)

      } else if (choice == 2) {
        //线性回归2
        val numIterations = 2000
        val stepSize = 0.1
        val model = LassoWithSGD.train(finalTrainingData, numIterations)


        finalTestData.map { point =>

          val prediction = model.predict(point.features)

          (prediction.toInt, point.label)

        }.zip(artistInfo)

      } else if (choice == 3) {
        //线性回归3
        val numIterations = 2000
        val stepSize = 0.1
        val model = RidgeRegressionWithSGD.train(finalTrainingData, numIterations)

        finalTestData.map { point =>

          val prediction = model.predict(point.features)
          (prediction.toInt, point.label)

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
          (prediction.toInt, point.label)

        }.zip(artistInfo)

      } else if (choice == 5) {
        //随机森林
        val categoricalFeaturesInfo = Map[Int, Int]()
        val numTrees = 200 // Use more in practice.
        val featureSubsetStrategy = "auto" // Let the algorithm choose.
        val impurity = "variance"
        val maxDepth = 12
        val maxBins = 50
        val model = RandomForest.trainRegressor(finalTrainingData, categoricalFeaturesInfo,
          numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)

        finalTestData.map { point =>

          val prediction = model.predict(point.features)
          (prediction.toInt, point.label)

        }.zip(artistInfo)

      }
      else if (choice == 6) {
        //梯度提升树
        val boostingStrategy = BoostingStrategy.defaultParams("Regression")
        boostingStrategy.setNumIterations(50)
        boostingStrategy.getTreeStrategy.setMaxDepth(20)
        //boostingStrategy.getTreeStrategy.setCategoricalFeaturesInfo(Map[Int, Int]())

        val model = GradientBoostedTrees.train(finalTrainingData, boostingStrategy)

        finalTestData.map { point =>

          val prediction = model.predict(point.features)
          (prediction.toInt, point.label)

        }.zip(artistInfo)

      }

    }







    val evaluateData = resultData.asInstanceOf[RDD[((Int, Double), (String, String))]]
      .map(t => ((t._2._2, t._2._1), (t._1._2, t._1._1)))


    val days = evaluateData.map { t => (t._1._2, 1) }.reduceByKey(_ + _).count()

    //(歌手,日期,实际播放数,预测值)
    val tempValue = evaluateData.map { t => (t._1._1, Math.pow((t._2._2 - t._2._1) / t._2._1, 2.0)) }.reduceByKey(_ + _)
    // val tempValue2 = evaluateData.map { t => (t._1._1, Math.pow((t._2._2 - t._2._1) / t._2._1, 2.0)) }
    val evaluateRDD = evaluateData.filter{t => Math.pow((t._2._2 - t._2._1) / t._2._1, 2.0)>=1}.map{t =>
      t._1._1+","+t._1._2+","+t._2._1+","+(t._2._2-t._2._1)+","+t._2._1
    }

    val writer = new PrintWriter(new File(args(3)))
    for(record <- evaluateRDD.collect()){
      writer.write(record)
      writer.println()
    }

    writer.close()


    //(歌手 方差)
    val fangcha = tempValue.map(t => (t._1, Math.sqrt(t._2 / days)))
    fangcha.collect().foreach(println)

    //(歌手 权重)
    val artistWeight = evaluateData.map { t => (t._1._1, t._2._1) }.reduceByKey(_ + _).map(t => (t._1, Math.sqrt(t._2)))

    val allScores = artistWeight.map(t => (1,t._2)).reduceByKey(_+_).map(t=>t._2).collect()(0)



    artistWeight.collect().foreach(println)
    val scores = artistWeight.zip(fangcha).filter(t => t._1._1 == t._2._1).map(t => (t._1._1, t._1._2 * (1.0 - t._2._2)))
      .map(t => (1, t._2)).reduceByKey(_ + _).map(t=>t._2).collect()(0)

    println("allScores:" + allScores)

    println("bili:"+ (scores/allScores)*100)

  }

}
