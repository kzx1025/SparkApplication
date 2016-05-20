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
      System.err.println("Usage of Parameters: master positiveData1 positiveData2 negativeData1 negativeData2 testData " +
        "model(1:LBFGS,2:SGD,3:DecisionTree) outputPath fraction")
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
    val data4 = sc.textFile(args(4))

    val dataA = data1 union data2
    val dataB = data3 union data4

    val data5 = sc.textFile(args(5))


    val rawTestData = data5.map{line =>
      val parts = line.split(",").drop(4).map(_.toDouble)

      LabeledPoint(parts(0), Vectors.dense(parts.slice(0, parts.length)))
    }

    val testUserData = data5.map{line =>
      val parts = line.split(",")
      (parts(0),parts(1),parts(2),parts(3))
    }


    val positiveData = dataA.map { line =>

      val parts = line.split(",").map(_.toDouble)

      LabeledPoint(parts(0), Vectors.dense(parts.slice(1, parts.length)))

    }

    val negativeData = dataB.map { line =>

      val parts = line.split(",").map(_.toDouble)

      LabeledPoint(parts(0), Vectors.dense(parts.slice(1, parts.length)))

    }

    val allData = positiveData union negativeData

   // val positiveDataNum = positiveData.count()
     // val negativeDataNum = negativeData.count()

    //标准正规化处理
    val scaler = new StandardScaler(withMean = true, withStd = true)
    val scaler2 = scaler.fit(allData.map(x => x.features))
    val scaler3 = scaler.fit(rawTestData.map(x => x.features))

    val finalData = allData.map(x => LabeledPoint(x.label, scaler2.transform(x.features)))

    val finalTestData = rawTestData.map(x => LabeledPoint(x.label, scaler3.transform(x.features)))


    val finalPositiveData = finalData.filter(x => x.label==1)
    val finalNegativeData = finalData.filter(x => x.label==0)
    //val finalPositiveData = sc.parallelize(finalData.take(positiveDataNum.toInt))
    //val finalNegativeData = sc.parallelize(finalData.collect().drop(positiveDataNum.toInt))
    //println(finalPositiveData.count() + "," + finalNegativeData.count())
    val fraction = finalPositiveData.count().toDouble/finalNegativeData.count().toDouble


    //正负样本采样
   // val positiveDataNum = finalPositiveData.count()
   // val samplePositiveData = sc.parallelize(finalPositiveData.takeSample(withReplacement = false, positiveDataNum.toInt, 42))
    val samplePositiveData = finalPositiveData
    val sampleNegativeData = finalNegativeData.sample(withReplacement = false,fraction*(args(8).toInt),42L)
   // val sampleNegativeData = sc.parallelize(finalNegativeData.takeSample(withReplacement = false, positiveDataNum.toInt*5, 42))



    val trainingData = samplePositiveData union sampleNegativeData

    //finalTestData.take(100).foreach(println)
   // testUserData.take(100).foreach(println)




    val choice = args(6).toInt

    val resultData = {
      if (choice == 1) {
        //逻辑回归 最大似然估计
        val model = new LogisticRegressionWithLBFGS().setNumClasses(2).run(trainingData)
       // model.save(sc,args(9))
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

    //resultData.asInstanceOf[RDD[_]].take(30).foreach(println)

/*   val writer = new PrintWriter(new File(args(8)))
    for(record <- resultData.asInstanceOf[RDD[_]].collect()) {

      val tempRecord = record.asInstanceOf[(Double,(String,String,String,String))]
      writer.write(tempRecord._1+","+tempRecord._2._1+","+tempRecord._2._2+","+tempRecord._2._3+","+tempRecord._2._4)
      writer.println()

    }

    writer.close()*/


    resultData.asInstanceOf[RDD[(Double,(String,String,String,String))]].map{t=>
      t._1.toString+","+t._2._1+","+t._2._2+","+t._2._3+","+t._2._4
    }.saveAsTextFile(args(7))

    println("finalnegative:"+sampleNegativeData.count())
    println("finalpositive:"+samplePositiveData.count())
    println("finalTestdata"+finalTestData.count())



  }


}
