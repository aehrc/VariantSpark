package au.csiro.variantspark
import org.apache.spark.rdd.RDD

package object data {
  implicit def toFeatueConverter[V](v:RDD[V]):ToFeature[V] = new ToFeature[V](v)
}