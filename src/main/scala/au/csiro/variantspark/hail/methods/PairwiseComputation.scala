package au.csiro.variantspark.hail.methods

import au.csiro.variantspark.algo.PairwiseOperation
import is.hail.methods.KinshipMatrix
import is.hail.variant.VariantDataset
import au.csiro.variantspark.hail.adapter.HailFeatureSource

object PairwiseComputation {
 
  def apply(vds: VariantDataset, pairwiseOp: PairwiseOperation): KinshipMatrix = {
    require(vds.wasSplit)
    val featureSource = HailFeatureSource(vds)
    val pairwiseResult = pairwiseOp.compute(featureSource.features.map(_.valueAsByteArray))
    
    val indexedRowMatrix = pairwiseResult.toIndexedRowMatrix(vds.hc.sc)
    println(s"sizes: ${indexedRowMatrix.numRows()}, ${indexedRowMatrix.numCols()}")
    KinshipMatrix(vds.hc, vds.sSignature, indexedRowMatrix,
        vds.sampleIds.toArray, vds.countVariants())
  }
  
}