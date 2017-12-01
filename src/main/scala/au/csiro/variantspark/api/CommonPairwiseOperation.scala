package au.csiro.variantspark.api

import au.csiro.variantspark.algo.metrics.ManhattanPairwiseMetric
import au.csiro.variantspark.algo.metrics.EuclideanPairwiseMetric
import au.csiro.variantspark.algo.PairwiseOperation
import au.csiro.variantspark.algo.metrics.SharedAltAlleleCount
import au.csiro.variantspark.algo.metrics.AtLeastOneSharedAltAlleleCount

object CommonPairwiseOperation  {
  
  val mapping = Map(
    "manhattan" ->  ManhattanPairwiseMetric,
    "euclidean" -> EuclideanPairwiseMetric,
    "sharedAltAlleleCount" -> SharedAltAlleleCount,
    "anySharedAltAlleleCount" -> AtLeastOneSharedAltAlleleCount
  )
  def withName(name:String):PairwiseOperation  = mapping(name)
}
