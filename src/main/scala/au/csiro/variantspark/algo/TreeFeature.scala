package au.csiro.variantspark.algo

import au.csiro.variantspark.data.DataLike
import au.csiro.variantspark.data.VariableType
import au.csiro.variantspark.data.Data
import au.csiro.variantspark.data.Feature
import au.csiro.variantspark.data.StdFeature
import org.apache.spark.rdd.RDD
import au.csiro.pbdava.ssparkle.spark.SparkUtils._

/**
 * Abstract representation for a feature as stored for the purpose of bulding decision trees.
 * The concrete implementation may choose to store data representation and or statistics
 * optmimized for the purpose of finding tree splits in this feature.
 * The implementation of splitting is expressed by implementing {{SplitterFactory}} 
 * (or {{FastSplitterFactory}} in case confusion based splitting is possible) 
 */
trait TreeFeature extends DataLike with SplitterProvider with Serializable {
  def label:String
  def variableType:VariableType
  def index: Long
  def toData: Data
  def toFeature: Feature = StdFeature(label, variableType, toData)
}

/**
 * Factory that encapsulates conversion from generic features to tree optimized features.
 */
trait TreeRepresentationFactory {
  def createRepresentation(f:RDD[(Feature,Long)]):RDD[TreeFeature] = f.map(fi => createRepresentation(fi._1, fi._2))
  /**
   * Create a tree optimized representation for the feature
   * @param f feature to optimize
   * @param i index of the feature
   * 
   * @return the tree optimized representation 
   */
  def createRepresentation(f:Feature, i:Long):TreeFeature
}

/**
 * Helper methods for {{RDD[TreeFeature]}}
 */
class TreeFeatureRDDFunction[V](val rdd:RDD[TreeFeature]) extends AnyVal {
  def size = rdd.first.size
  def collectAtIndexes(indexes:Set[Long]):Map[Long, Data] = withBroadcast(rdd)(indexes) { br_indexes =>
      rdd.filter({ case tf => br_indexes.value.contains(tf.index)})
        .map(tf => (tf.index, tf.toData))
        .collectAsMap().toMap
  }
}
