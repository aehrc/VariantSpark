package au.csiro.variantspark.algo

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vector
import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap
import au.csiro.pbdava.ssparkle.common.utils.FastUtilConversions._

trait PredictiveModel {
    
  def predict(data: RDD[Vector]): Array[Int] = predictIndexed(data.zipWithIndex())
  def predictIndexed(data: RDD[(Vector,Long)]): Array[Int] 
  def printout() 
}

trait PredictiveModelWithImportance extends PredictiveModel {
  def variableImportanceAsFastMap: Long2DoubleOpenHashMap 
  def variableImportance(): Map[Long, Double] =  variableImportanceAsFastMap.asScala
}