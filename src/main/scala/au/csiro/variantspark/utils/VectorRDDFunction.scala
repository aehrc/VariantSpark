package au.csiro.variantspark.utils

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vector

class VectorRDDFunction(val rdd:RDD[Vector]) extends AnyVal {
  def project(p:Projector):RDD[Vector] = rdd.map(p.projectVector)
}

object VectorRDDFunction {
  implicit def toVectorRDD(rdd:RDD[Vector]) = new VectorRDDFunction(rdd)
}