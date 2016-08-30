package au.csiro.variantspark.tests

import au.csiro.pbdava.ssparkle.spark.SparkApp
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import au.csiro.variantspark.metrics.Metrics
import au.csiro.variantspark.utils.Projector
import au.csiro.variantspark.utils.VectorRDDFunction._
import au.csiro.variantspark.utils.CV
import au.csiro.variantspark.algo.WideRandomForest
import scala.Range

object TestWideDecisionTree extends SparkApp {
  conf.setAppName("VCF cluster")

  
  def main(args:Array[String]) {
    println("Testign WideKMeans")
    
    
    val dims = 1000
    val importantDims = 30
    val centersNo = 3
    val samples = 100
    val clusterVariance = 0.005
    val centers = sc.parallelize(Range(0,dims).map(i => Vectors.dense(Array.fill(centersNo)((Math.random()*3).toInt.toDouble))), 10)
    //centers.foreach(println)
    val clusterAssignment = Range(0,samples).map(i => Math.floor(Math.random()*centersNo).toInt).toList
    println(clusterAssignment)
    
    val vectorData:RDD[Vector] = centers.zipWithIndex().map{ case (v,i) =>
      if (i< importantDims) Vectors.dense(clusterAssignment.map(c =>
        ((v(c).toInt + (Math.random()*1.3).toInt) % centersNo).toDouble).toArray)
      else 
        Vectors.dense(Array.fill(samples)((Math.random()*3).toInt.toDouble))
    }
    
    val labels = clusterAssignment.toArray
    
    val (trainSetProj, testSetProj) = Projector.splitRDD(vectorData, 0.8)
    val trainSetWithIndex = vectorData.project(trainSetProj).zipWithIndex().cache()
    val trainLables = trainSetProj.projectArray(labels)

    val testSet = vectorData.project(testSetProj).cache()
    val testLables = testSetProj.projectArray(labels)

/*    
    val testLabels = Range(0,samples).map(i => Math.floor(Math.random()*centersNo).toInt).toList
    val testData:RDD[Vector] = centers.zipWithIndex().map{ case (v,i) =>
      if (i< importantDims) Vectors.dense(testLabels.map(c =>
        ((v(c).toInt + (Math.random()*1.3).toInt) % centersNo).toDouble).toArray)
      else 
        Vectors.dense(Array.fill(samples)((Math.random()*3).toInt.toDouble))
    }    
    
    val test = data.count()
    println("Records to process: "+ test)
*/    
    val rf = new WideRandomForest()
    val result  = rf.train(trainSetWithIndex, trainLables, 20)
    //println(result)
    //result.printout()
    val variableImportnace = result.variableImportance
    println(result.predict(testSet).toList)    
    variableImportnace.toSeq.sortBy(-_._2).take(50).foreach(println)
    
    val testPredict = result.predict(testSet)
    val testError = Metrics.classificatoinError(testLables,testPredict)
    println(s"Test error: ${testError}")
  
   // now try cross validatio
   val cvResult = CV.evaluate(Projector.rddFolds(vectorData, 3)) { fold =>
      val trainSetWithIndex = vectorData.project(fold.inverted).zipWithIndex().cache()
      val trainLables = fold.inverted.projectArray(labels)

      val testSet = vectorData.project(fold).cache()
      val testLables = fold.projectArray(labels)   
     
      val rf = new WideRandomForest()
      val result  = rf.train(trainSetWithIndex, trainLables, 20)
      val testPredict = result.predict(testSet)
      val testError = Metrics.classificatoinError(testLables,testPredict)
      testError
    }
    
    println(s"CV error: ${cvResult}")
  
  }
}