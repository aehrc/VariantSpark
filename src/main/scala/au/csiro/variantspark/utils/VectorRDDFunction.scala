package au.csiro.variantspark.utils

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vector

class VectorRDDFunction(val rdd:RDD[Vector]) extends AnyVal {
  def project(p:Projector):RDD[Vector] = rdd.map(p.projectVector)
}

class IndexedVectorRDDFunction(val rdd:RDD[(Vector, Long)]) extends AnyVal {
  def project(p:Projector):RDD[(Vector, Long)] = rdd.map(t => (p.projectVector(t._1),t._2))
}

object VectorRDDFunction {
  implicit def toVectorRDD(rdd:RDD[Vector]) = new VectorRDDFunction(rdd)
}