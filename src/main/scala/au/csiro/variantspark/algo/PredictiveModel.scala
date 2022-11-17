package au.csiro.variantspark.algo

import au.csiro.pbdava.ssparkle.common.utils.FastUtilConversions._
import au.csiro.variantspark.data.Feature
import it.unimi.dsi.fastutil.longs.{Long2DoubleOpenHashMap, Long2LongOpenHashMap}
import org.apache.spark.rdd.RDD

trait PredictiveModel {
  def predict(data: RDD[(Feature, Long)]): Array[Int]
  def printout()
}

trait PredictiveModelWithImportance extends PredictiveModel {
  def variableImportanceAsFastMap: Long2DoubleOpenHashMap
  def variableSplitCountAsFastMap: Long2LongOpenHashMap
  def variableImportance(): Map[Long, Double] = variableImportanceAsFastMap.asScala
  def variableSplitCount(): Map[Long, Long] = variableSplitCountAsFastMap.asScala
}


/** REGRESSION */
trait RegressionPredictiveModel {
  def predict(data: RDD[(Feature, Long)]): Array[Double]
  def printout()
}

trait RegressionPredictiveModelWithImportance extends RegressionPredictiveModel {
  def variableImportanceAsFastMap: Long2DoubleOpenHashMap
  def variableSplitCountAsFastMap: Long2LongOpenHashMap
  def variableImportance(): Map[Long, Double] = variableImportanceAsFastMap.asScala
  def variableSplitCount(): Map[Long, Long] = variableSplitCountAsFastMap.asScala
}
