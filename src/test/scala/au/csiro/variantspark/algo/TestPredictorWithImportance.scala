package au.csiro.variantspark.algo

import org.apache.spark.rdd.RDD
import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap
import org.apache.spark.mllib.linalg.Vector

case class TestPredictorWithImportance(val predictions:Array[Int], val importance:Long2DoubleOpenHashMap) extends PredictiveModelWithImportance {
  def predictIndexed(data: RDD[(Vector,Long)]): Array[Int]  = predictions
  def variableImportanceAsFastMap: Long2DoubleOpenHashMap = importance
  def printout() {}
}