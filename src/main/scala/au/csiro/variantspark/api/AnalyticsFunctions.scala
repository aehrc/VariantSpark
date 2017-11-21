package au.csiro.variantspark.api

import au.csiro.variantspark.input.FeatureSource
import au.csiro.variantspark.input.LabelSource

class AnalyticsFunctions(val featureSource: FeatureSource) extends AnyVal {
  
  def importanceAnalysis(labelSource:LabelSource, nTrees:Int = 1000, 
        mtryFraction:Option[Double] = None, oob:Boolean = true,
        seed: Option[Long] = None, batchSize:Int = 100, varOrdinalLevels:Int = 3
        )(implicit vsContext:SqlContextHolder): ImportanceAnalysis = {
    ImportanceAnalysis(featureSource, labelSource,  nTrees,  mtryFraction, 
        oob, seed, batchSize, varOrdinalLevels)
  }
}