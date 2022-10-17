package au.csiro.variantspark.algo

import au.csiro.variantspark.algo.impurity.GiniImpurityAggregator

/**
  * Gini impurity measure
  */
case object GiniImpurity extends ClassificationImpurity {
  def createAggregator(nCategories: Int): ClassificationImpurityAggregator =
    new GiniImpurityAggregator(nCategories)
}
