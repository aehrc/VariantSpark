package au.csiro.variantspark.api

import au.csiro.variantspark.input.FeatureSource
import au.csiro.variantspark.input.LabelSource
import au.csiro.variantspark.algo.PairwiseOperation
import au.csiro.variantspark.algo.LowerTriangMatrix
import au.csiro.variantspark.algo.RandomForestModel
import org.apache.spark.SparkContext

/**
  * Extends a feature with analytical functions.
  */
class AnalyticsFunctions(val featureSource: FeatureSource) extends AnyVal {

  /**
    * Builds random forest classifier for the provided labels and estimates variable
    * importance using gini importance.
    *
    * @param rfModel: The trained random forest model.
    *
    * @return [[au.csiro.variantspark.api.ImportanceAnalysis.apply]] importance analysis model
    */
  def importanceAnalysis(rfModel: RandomForestModel)(
      implicit vsContext: SqlContextHolder): ImportanceAnalysis = {
    ImportanceAnalysis(featureSource, rfModel)
  }

  /**
    * Computes a pairwise operation on samples.
    *
    * @param operation: a pairwise operation.
    *
    * @return [[au.csiro.variantspark.algo.LowerTriangMatrix]] lower triangular
    *        matrix with the result of the
    *       pairwise computation. The result includes the diagonal.
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
    * @param  operationName: name of the operation. One of `manhattan`, `euclidean`,
    *     `sharedAltAlleleCount`, `anySharedAltAlleleCount`
    *
    *  @return [[au.csiro.variantspark.algo.LowerTriangMatrix]] lower triangular matrix
    *        with the result of the
    *        pairwise computation. The result includes the diagonal.
    */
  def pairwiseOperation(operationName: String): LowerTriangMatrix = {
    CommonPairwiseOperation
      .withName(operationName)
      .compute(featureSource.features.map(_.valueAsByteArray))
  }
}
