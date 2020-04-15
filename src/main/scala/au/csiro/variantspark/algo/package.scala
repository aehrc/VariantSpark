package au.csiro.variantspark

import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import au.csiro.variantspark.utils.CanSize
import au.csiro.variantspark.data.Feature

package object algo {
  type IndexedFeature = (Feature, Long)

  implicit case object CanSizeFeature extends CanSize[Feature] {
    override def size(f: Feature): Int = f.size
    override def runtimeClass: Class[_] = classOf[Feature]
  }

  implicit def toTreeFeatueRDD(rdd: RDD[TreeFeature]): TreeFeatureRDDFunction[Nothing] =
    new TreeFeatureRDDFunction(rdd)
}
