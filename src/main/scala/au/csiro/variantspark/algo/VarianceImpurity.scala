package au.csiro.variantspark.algo

import au.csiro.variantspark.algo.impurity.VarianceImpurityAggregator

/**
  * Variance impurity measure
  */
case object VarianceImpurity extends RegressionImpurity {
  def createAggregator(): RegressionImpurityAggregator =
    new VarianceImpurityAggregator()
}