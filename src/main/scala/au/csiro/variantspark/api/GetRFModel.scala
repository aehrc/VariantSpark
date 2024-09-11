package au.csiro.variantspark.api

import au.csiro.variantspark.algo.{
  RandomForest,
  RandomForestModel,
  RandomForestParams
}
import au.csiro.variantspark.input.{FeatureSource, LabelSource}

/** Passes a trained random forest model back to the python wrapper
  */
object RFModelTrainer {
  def trainModel(
      featureSource: FeatureSource,
      labelSource: LabelSource,
      params: RandomForestParams,
      nTrees: Int,
      rfBatchSize: Int
  ): RandomForestModel = {
    val labels = labelSource.getLabels(featureSource.sampleNames)
    lazy val inputData = featureSource.features.zipWithIndex.cache()
    val rf = new RandomForest(params)
    val rfTrained = rf.batchTrain(inputData, labels, nTrees, rfBatchSize)
    rfTrained
  }
}
