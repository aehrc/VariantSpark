package au.csiro.variantspark.api

import au.csiro.variantspark.input.FeatureSource
import au.csiro.variantspark.input.LabelSource
import au.csiro.variantspark.algo.PairwiseOperation
import au.csiro.variantspark.algo.LowerTriangMatrix

/**
  * Extends a feature with analytical functions.
  */
class AnalyticsFunctions(val featureSource: FeatureSource) extends AnyVal {

  /**
    * Builds random forest classifier for the provided labels and estimates variable
    * importance using gini importance.
	 *
    * @param labelSource: labels to use for importance analysis.
    * @param nTrees: the number of trees to build in the forest.
    * @param mtryFraction: the fraction of variables to try at each split.
    * @param oob: should OOB (Out of Bag) error estimate be calculated.
    * @param seed: random seed to use.
    * @param batchSize: the number of trees to build in one batch.
    * @param varOrdinalLevels: the number levels in the ordinal features.
    *
    * @return [[au.csiro.variantspark.api.ImportanceAnalysis.apply]] importance analysis model
    */
  def importanceAnalysis(labelSource: LabelSource, nTrees: Int = 1000,
      mtryFraction: Option[Double] = None, oob: Boolean = true, seed: Option[Long] = None,
      batchSize: Int = 100, varOrdinalLevels: Int = 3)(
      implicit vsContext: SqlContextHolder): ImportanceAnalysis = {
    ImportanceAnalysis(featureSource, labelSource, nTrees, mtryFraction, oob, seed, batchSize,
      varOrdinalLevels)
  }

  /**
    * Computes a pairwise operation on samples.
    *
    * @param opeataion: a pairwise operation.
    *
	 * @return [[au.csiro.variantspark.algo.LowerTriangMatrix]] lower triangular matrix with the result of the
	 * 			pairwise computation. The result includes the diagonal.
    */
  def pairwiseOperation(operation: PairwiseOperation): LowerTriangMatrix = {
    operation.compute(featureSource.features.map(_.valueAsByteArray))
  }

  /**
    * Computes a pairwise operation on samples. Currently implemented operations
    * include:
    * - `manhattan` : the Manhattan distance
    * - `euclidean` : the Euclidean distance
    * - `sharedAltAlleleCount`: count of shared alternative alleles
    * - `anySharedAltAlleleCount`: count of variants that share at least one alternative allele
    *
    * @param  operation_name: name of the operation. One of `manhattan`, `euclidean`,
    * 		`sharedAltAlleleCount`, `anySharedAltAlleleCount`
	 *
	 * @return [[au.csiro.variantspark.algo.LowerTriangMatrix]] lower triangular matrix with the result of the
	 * 			pairwise computation. The result includes the diagonal.
    */
  def pairwiseOperation(operationName: String): LowerTriangMatrix = {
    CommonPairwiseOperation
      .withName(operationName)
      .compute(featureSource.features.map(_.valueAsByteArray))
  }
}
