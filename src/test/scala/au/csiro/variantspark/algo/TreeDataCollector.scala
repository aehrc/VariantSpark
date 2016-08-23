package au.csiro.variantspark.algo

import scala.collection.mutable.MutableList

import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD

 import au.csiro.variantspark.utils.Sample

 class TreeDataCollector(treeStream:Stream[PredictiveModelWithImportance] = Stream.continually(TestPredictorWithImportance(null, null))) {  
  val allData = MutableList[RDD[(Vector, Long)]]()
  val allLabels = MutableList[Array[Int]]()
  val allnTryFration = MutableList[Double]()
  val allSamples = MutableList[Sample]()
  val allTreest = MutableList[PredictiveModelWithImportance]()
  val treeIter = treeStream.toIterator
  
  def collectData(indexedData: RDD[(Vector, Long)], labels: Array[Int], nTryFraction: Double, sample:Sample) = {
    allData += indexedData
    allLabels += labels
    allnTryFration += nTryFraction
    allSamples += sample
    allTreest += treeIter.next()
    allTreest.last
  }
}