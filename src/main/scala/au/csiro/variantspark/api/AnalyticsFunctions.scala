package au.csiro.variantspark.api

import au.csiro.variantspark.input.FeatureSource
import au.csiro.variantspark.input.LabelSource
import au.csiro.variantspark.algo.PairwiseOperation
import au.csiro.variantspark.algo.UpperTriangMatrix

/**
 * Extends a feature with analytical functions.
 */
class AnalyticsFunctions(val featureSource: FeatureSource) extends AnyVal {
  

  def importanceAnalysis(labelSource:LabelSource, nTrees:Int = 1000, 
        mtryFraction:Option[Double] = None, oob:Boolean = true,
        seed: Option[Long] = None, batchSize:Int = 100, varOrdinalLevels:Int = 3
        )(implicit vsContext:SqlContextHolder): ImportanceAnalysis = {
    ImportanceAnalysis(featureSource, labelSource,  nTrees,  mtryFraction, 
        oob, seed, batchSize, varOrdinalLevels)
  }
  
  /**
   * Computes a pairwise operation on samples. 
   * 
   * @param opeataion: a pairwise operation.
   * 
	 * @return [[au.csiro.variantspark.algo.UpperTriangMatrix]] upper triangular matrix with the result of the 
	 * 			pairwise computation. The result includes the diagonal. 
   */  
  def pairwiseOperation(operation:PairwiseOperation):UpperTriangMatrix = {
    operation.compute(featureSource.features().map(_.values))
  }
 
  /**
   * Computes a pairwise operation on samples. Currently implemented operations 
   * include:
   * - `manhattan` : the Manhattan distance
   * - `euclidean` : the Euclidean distance
   * - `sharedAltAlleleCount`: count of shared alternative alleles
   * - `anySharedAltAlleleCount`: count of variants that share at least one alternative allele
   * 
   * @param  operation_name: name of the operaiton. One of `manhattan`, `euclidean`, 
   * 		`sharedAltAlleleCount`, `anySharedAltAlleleCount`
	 * 
	 * @return [[au.csiro.variantspark.algo.UpperTriangMatrix]] upper triangular matrix with the result of the 
	 * 			pairwise computation. The result includes the diagonal. 
   */
  def pairwiseOperation(operationName: String):UpperTriangMatrix = {
    CommonPairwiseOperation.withName(operationName).compute(featureSource.features().map(_.values))
  }
 
}