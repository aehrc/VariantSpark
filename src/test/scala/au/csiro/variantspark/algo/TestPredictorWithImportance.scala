package au.csiro.variantspark.algo

import au.csiro.variantspark.data.Feature
import it.unimi.dsi.fastutil.longs.{Long2DoubleOpenHashMap, Long2LongOpenHashMap}
import org.apache.spark.rdd.RDD

case class TestPredictorWithImportance(val predictions: Array[Int],
    val importance: Long2DoubleOpenHashMap, val splitCounts: Long2LongOpenHashMap)
    extends PredictiveModelWithImportance {

  def predict(data: RDD[(Feature, Long)]): Array[Int] = predictions
  override def variableImportanceAsFastMap: Long2DoubleOpenHashMap = importance
  override def variableSplitCountAsFastMap: Long2LongOpenHashMap = splitCounts

  def printout() {}
  implicit def toMember: RandomForestMember = RandomForestMember(this)

}
