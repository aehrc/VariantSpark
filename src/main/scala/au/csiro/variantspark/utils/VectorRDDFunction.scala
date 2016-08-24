package au.csiro.variantspark.utils

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vector
import au.csiro.pbdava.ssparkle.spark.SparkUtils._

class VectorRDDFunction(val rdd:RDD[Vector]) extends AnyVal {
  def project(p:Projector):RDD[Vector] = rdd.map(p.projectVector)
}

class IndexedVectorRDDFunction(val rdd:RDD[(Vector, Long)]) extends AnyVal {
  def project(p:Projector):RDD[(Vector, Long)] = rdd.map(t => (p.projectVector(t._1),t._2))
  def size = rdd.first()._1.size
  def collectAtIndexes(indexes:Set[Long]):Map[Long, Vector] = withBrodcast(rdd)(indexes) { br_indexes =>
      rdd.filter({ case (data,variableIndex) => br_indexes.value.contains(variableIndex)})
        .map(_.swap)
        .collectAsMap().toMap
  }
}

object VectorRDDFunction {
  implicit def toVectorRDD(rdd:RDD[Vector]) = new VectorRDDFunction(rdd)
  implicit def toIndexedVectorRDD(rdd:RDD[(Vector, Long)]) = new IndexedVectorRDDFunction(rdd)
}