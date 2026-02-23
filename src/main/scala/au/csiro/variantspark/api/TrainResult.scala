package au.csiro.variantspark.api

import au.csiro.variantspark.data.Feature
import org.apache.spark.rdd.RDD

/** Result of training containing both the model and the persisted indexed
  * feature RDD used during training. The caller is responsible for calling
  * `releaseIndexedData()` when done with importance analysis / export.
  */
case class TrainResult(model: au.csiro.variantspark.algo.RandomForestModel,
    indexedFeatures: RDD[(Feature, Long)]) {
  def releaseIndexedData(): Unit = indexedFeatures.unpersist()
}
