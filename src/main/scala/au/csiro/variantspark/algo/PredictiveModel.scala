package au.csiro.variantspark.algo

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vector
import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap
import au.csiro.pbdava.ssparkle.common.utils.FastUtilConversions._
import scala.reflect.ClassTag

trait PredictiveModel[V] {
    
  def predict(data: RDD[V])(implicit ct:ClassTag[V]): Array[Int] = predictIndexed(data.zipWithIndex())
  def predictIndexed(data: RDD[(V,Long)])(implicit ct:ClassTag[V]): Array[Int] 
  def printout() 
}

trait PredictiveModelWithImportance[V] extends PredictiveModel[V] {
  def variableImportanceAsFastMap: Long2DoubleOpenHashMap 
  def variableImportance(): Map[Long, Double] =  variableImportanceAsFastMap.asScala
}