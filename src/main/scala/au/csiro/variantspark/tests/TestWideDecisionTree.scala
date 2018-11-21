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
import au.csiro.variantspark.data.BoundedOrdinalVariable
import au.csiro.variantspark.algo.RandomForestParams
import au.csiro.variantspark.algo.RandomForest

object TestWideDecisionTree extends SparkApp {
  conf.setAppName("VCF cluster")
  
  def main(args:Array[String]) {
    println("Testing WideKMeans")
    
    val dims = 1000
    val importantDims = 30
    val centersNo = 3
    val samples = 100
    val clusterVariance = 0.005
    val centers = sc.parallelize(Range(0,dims).map(i => Vectors.dense(Array.fill(centersNo)((Math.random()*3).toInt.toDouble))), 10)
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
    val trainLabels = trainSetProj.projectArray(labels)

    val testSet = vectorData.project(testSetProj).cache()
    val testLabels = testSetProj.projectArray(labels)
    
    val dataType = BoundedOrdinalVariable(3)
    val rf = new WideRandomForest()
    
    val result  = rf.batchTrain(trainSetWithIndex, dataType, trainLabels, 20, 10)

    val variableImportance = result.variableImportance
    println(result.predict(testSet).toList)    
    variableImportance.toSeq.sortBy(-_._2).take(50).foreach(println)
    
    val testPredict = result.predict(testSet)
    val testError = Metrics.classificationError(testLabels,testPredict)
    println(s"Test error: ${testError}")

   val crossvalidateResult = CV.evaluateMean(Projector.rddFolds(vectorData, 3)) { fold =>
      val trainSetWithIndex = vectorData.project(fold.inverted).zipWithIndex().cache()
      val trainLabels = fold.inverted.projectArray(labels)

      val testSet = vectorData.project(fold).cache()
      val testLabels = fold.projectArray(labels)
     
      val rf = new WideRandomForest()
      val result  = rf.batchTrain(trainSetWithIndex,dataType,  trainLabels, 20, 10)
      val testPredict = result.predict(testSet)
      val testError = Metrics.classificationError(testLabels,testPredict)
      testError
    }
    
    println(s"Cross Validation error: ${crossvalidateResult}")
  
  }
}