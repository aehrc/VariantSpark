package au.csiro.variantspark.api

import au.csiro.variantspark.algo.{RandomForest, RandomForestModel, RandomForestParams}
import au.csiro.variantspark.input.{FeatureSource, LabelSource}

/** Passes a trained random forest model back to the python wrapper
  */
object RFModelTrainer {

  /** Trains a random forest model with provided data and parameters
    *
    * @param featureSource: FeatureSource object containing training X
    * @param labelSource: LabelSource object containing training y
    * @param params: Random forest hyperparameters (passed to model on initialisation)
    * @param nTrees: Number of trees to compute (passed to model during training)
    * @param rfBatchSize: Number of trees per batch (passed to model during training)
    *
    * @return Trained random forest model
    */
  def trainModel(featureSource: FeatureSource, labelSource: LabelSource,
      params: RandomForestParams, nTrees: Int, rfBatchSize: Int): RandomForestModel = {
    val labels = labelSource.getLabels(featureSource.sampleNames)
    lazy val inputData = featureSource.features.zipWithIndex.cache()
    val rf = new RandomForest(params)
    val rfTrained = rf.batchTrain(inputData, labels, nTrees, rfBatchSize)
    rfTrained
  }
}
