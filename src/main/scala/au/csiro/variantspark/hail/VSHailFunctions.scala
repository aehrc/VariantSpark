package au.csiro.variantspark.hail

import is.hail.variant.VariantDataset
import au.csiro.variantspark.hail.methods.RfImportanceAnalysis
import au.csiro.variantspark.algo.PairwiseOperation
import is.hail.methods.KinshipMatrix
import au.csiro.variantspark.hail.methods.PairwiseComputation
import au.csiro.variantspark.api.CommonPairwiseOperation


class VSHailFunctions(val vds:VariantDataset) extends AnyVal {
  
  private def requireSplit(methodName: String) {
    if (!vds.wasSplit)
      throw new IllegalStateException(s"method `$methodName' requires a split dataset. Use `split_multi' or `filter_multi' first.")
  }
  
  def importanceAnalysis(y: String, nTrees:Int=1000, 
        mtryFraction:Option[Double] = None, oob:Boolean = true, seed: Option[Long] = None, batchSize:Int = 100):RfImportanceAnalysis = {
    requireSplit("importance analysis")
    RfImportanceAnalysis(vds, y, nTrees, mtryFraction, oob,  seed, batchSize)
  }
  
  def pairwiseOperation(op: PairwiseOperation):KinshipMatrix  = { 
    requireSplit("pairwise operation")
    PairwiseComputation(vds, op)
  }

  def pairwiseOperation(operationName:String):KinshipMatrix = { 
    pairwiseOperation(CommonPairwiseOperation.withName(operationName))
  }
  
}

