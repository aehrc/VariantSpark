package au.csiro.variantspark.api

import au.csiro.variantspark.algo.metrics.ManhattanPairwiseMetric
import au.csiro.variantspark.algo.metrics.EuclideanPairwiseMetric
import au.csiro.variantspark.algo.{AggregablePairwiseOperation, PairwiseOperation}
import au.csiro.variantspark.algo.metrics.SharedAltAlleleCount
import au.csiro.variantspark.algo.metrics.AtLeastOneSharedAltAlleleCount

object CommonPairwiseOperation {

  /**
    * Mapping class for commong pairwise operations
    */
  val mapping: Map[String, AggregablePairwiseOperation] = Map(
      "manhattan" -> ManhattanPairwiseMetric, "euclidean" -> EuclideanPairwiseMetric,
      "sharedAltAlleleCount" -> SharedAltAlleleCount,
      "anySharedAltAlleleCount" -> AtLeastOneSharedAltAlleleCount)

  /**
    * Find a pairwise operation with give name.
    *
    * @param name: the name of the operation to return.
    * @return returns a [[au.csiro.variantspark.algo.PairwiseOperation]] with specified name.
    */
  def withName(name: String): PairwiseOperation = mapping(name)
}
