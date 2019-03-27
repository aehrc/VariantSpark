package au.csiro.variantspark.algo

import au.csiro.pbdava.ssparkle.common.utils.FastUtilConversions._
import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag
import au.csiro.variantspark.data.Feature

trait PredictiveModel {
  def predict(data: RDD[(Feature, Long)]): Array[Int]  
  def printout() 
}

trait PredictiveModelWithImportance extends PredictiveModel {
  def variableImportanceAsFastMap: Long2DoubleOpenHashMap 
  def variableImportance(): Map[Long, Double] =  variableImportanceAsFastMap.asScala
}