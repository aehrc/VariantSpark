package au.csiro.variantspark.algo

import au.csiro.variantspark.metrics.{Gini, Variance}
import it.unimi.dsi.util.XorShift1024StarRandomGenerator
import org.apache.commons.math3.util.MathArrays

/**
  * The type of machine learning problem. Acts as a factory for [[ImpurityCalculator]],
  * ensuring the response type is compatible with the chosen impurity measure.
  */
sealed trait ProblemType {
  def makeCalculator(response: ResponseVariable): ImpurityCalculator
}

/** Classification using Gini impurity. */
case object Classification extends ProblemType {
  def makeCalculator(response: ResponseVariable): ImpurityCalculator = response match {
    case ClassificationResponse(labels) => new GiniCalculator(labels)
    case _ =>
      throw new IllegalArgumentException("Classification requires ClassificationResponse")
  }
}

/** Regression using variance impurity. */
case object Regression extends ProblemType {
  def makeCalculator(response: ResponseVariable): ImpurityCalculator = response match {
    case RegressionResponse(values) => new VarianceCalculator(values)
    case _ =>
      throw new IllegalArgumentException("Regression requires RegressionResponse")
  }
}

/**
  * Bridges a [[ResponseVariable]] with the impurity measure and splitting
  * infrastructure for one training run. Created once per tree and reused
  * across all splits.
  */
trait ImpurityCalculator extends Serializable {
  def length: Int
  def calculate(splitIndices: Array[Int]): ImpurityStats
  def createSplitterFactory(): IndexedSplitterFactory
  def createPredictionAggregatorFactory(): PredictionAggregatorFactory
  def permute(rng: XorShift1024StarRandomGenerator): (ImpurityCalculator, Array[Int])
}

/**
  * [[ImpurityCalculator]] for classification using Gini impurity.
  * @param labels integer class labels for all samples (0-based)
  */
class GiniCalculator(labels: Array[Int]) extends ImpurityCalculator {
  val nCategories: Int = labels.max + 1

  def length: Int = labels.length

  def calculate(indices: Array[Int]): ImpurityStats = {
    val (totalImpurity, classCounts) = Gini.giniImpurity(indices, labels, nCategories)
    ClassificationStats(totalImpurity, indices.length, classCounts)
  }

  def createSplitterFactory(): IndexedSplitterFactory = {
    val agg = ClassificationSplitAggregator(GiniImpurity, labels, nCategories)
    val ca = new ClassificationLevelAggregator(GiniImpurity, 10, nCategories, labels)
    new DefStatefulIndexedSplitterFactory(agg, Some(ca))
  }

  def createPredictionAggregatorFactory(): PredictionAggregatorFactory =
    VotingAggregatorFactory(nCategories)

  def permute(rng: XorShift1024StarRandomGenerator): (ImpurityCalculator, Array[Int]) = {
    val permutationOrder = labels.indices.toArray
    MathArrays.shuffle(permutationOrder, rng)
    (new GiniCalculator(permutationOrder.map(labels(_))), permutationOrder)
  }
}

/**
  * [[ImpurityCalculator]] for regression using variance impurity.
  * @param values continuous target values for all samples
  */
class VarianceCalculator(values: Array[Double]) extends ImpurityCalculator {
  def length: Int = values.length

  def calculate(indices: Array[Int]): ImpurityStats = {
    val (totalImpurity, size, sum, sumOfSquares) = Variance.varianceImpurity(indices, values)
    RegressionStats(totalImpurity, size, sum, sumOfSquares)
  }

  def createSplitterFactory(): IndexedSplitterFactory = {
    val agg = RegressionSplitAggregator(VarianceImpurity, values)
    val ca = new RegressionLevelAggregator(VarianceImpurity, 10, values)
    new DefStatefulIndexedSplitterFactory(agg, Some(ca))
  }

  def createPredictionAggregatorFactory(): PredictionAggregatorFactory =
    AveragingAggregatorFactory

  def permute(rng: XorShift1024StarRandomGenerator): (ImpurityCalculator, Array[Int]) = {
    val permutationOrder = values.indices.toArray
    MathArrays.shuffle(permutationOrder, rng)
    (new VarianceCalculator(permutationOrder.map(values(_))), permutationOrder)
  }
}
