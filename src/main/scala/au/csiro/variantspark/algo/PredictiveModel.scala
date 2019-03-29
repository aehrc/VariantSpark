package au.csiro.variantspark.algo

import au.csiro.pbdava.ssparkle.common.utils.FastUtilConversions._
import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

trait PredictiveModel[V] {
    
  def predict(data: RDD[V])(implicit canSplit:CanSplit[V]): Array[Int] = predictIndexed(data.zipWithIndex())
  def predictIndexed(data: RDD[(V,Long)])(implicit canSplit:CanSplit[V]): Array[Int] 
  def printout() 
}

trait PredictiveModelWithImportance[V] extends PredictiveModel[V] {
  def variableImportanceAsFastMap: Long2DoubleOpenHashMap 
  def variableImportance(): Map[Long, Double] =  variableImportanceAsFastMap.asScala
}