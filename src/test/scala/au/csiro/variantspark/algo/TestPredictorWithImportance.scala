package au.csiro.variantspark.algo

import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag
import au.csiro.variantspark.data.Feature

case class TestPredictorWithImportance(val predictions: Array[Int], val importance: Long2DoubleOpenHashMap) extends PredictiveModelWithImportance {
 
  def predict(data: RDD[(Feature, Long)]): Array[Int] = predictions
  def variableImportanceAsFastMap: Long2DoubleOpenHashMap = importance
  def printout() {}
  implicit def toMember = RandomForestMember(this)
}