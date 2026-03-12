package au.csiro.variantspark.api

import au.csiro.variantspark.algo.{RandomForest, RandomForestModel, RandomForestParams}
import au.csiro.variantspark.data.Feature
import au.csiro.variantspark.input.{FeatureSource, ResponseSource}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

/** Passes a trained random forest model back to the python wrapper
  */
object RFModelTrainer {

  /** Trains a random forest model with provided data and parameters.
    *
    * Returns a [[TrainResult]] containing both the model and the persisted
    * indexed feature RDD. The indexed RDD should be reused for importance
    * analysis and model export to ensure consistent index-to-label mapping.
    * The caller is responsible for calling `releaseIndexedData()` when done.
    *
    * @param featureSource: FeatureSource object containing training X
    * @param responseSource: ResponseSource object containing training y
    * @param params: Random forest hyperparameters (passed to model on initialisation)
    * @param nTrees: Number of trees to compute (passed to model during training)
    * @param rfBatchSize: Number of trees per batch (passed to model during training)
    * @param nPartitions: optional override for the number of partitions (0 = auto)
    *
    * @return TrainResult with trained model and persisted indexed features
    */
  def trainModel(featureSource: FeatureSource, responseSource: ResponseSource[Int],
      params: RandomForestParams, nTrees: Int, rfBatchSize: Int,
      nPartitions: Int = 0): TrainResult = {
    val responses = responseSource.getResponses(featureSource.sampleNames)

    // Deterministically repartition and index features using MurMur3 hash
    // on feature labels. This ensures reproducible index assignments.
    val indexedFeatures = FeatureIndexer.index(featureSource.features, nPartitions)

    val rf = new RandomForest(params)
    val rfTrained = rf.batchTrain(indexedFeatures, responses, nTrees, rfBatchSize)

    // Return both model and indexed data; caller manages lifecycle
    TrainResult(rfTrained, indexedFeatures)
  }
}
