package au.csiro.variantspark.algo

/**
  * Helper class to return impurity of a split
  */
class SplitImpurity(var left: Double, var right: Double) {
  def this() {
    this(0.0, 0.0)
  }
  def set(left: Double, right: Double) {
    this.left = left
    this.right = right
  }
}

/**
  * Mutable class that encapsulates the logic for computing a specific impurity measure.
  * Should maintain state needed to compute the impurity at any given moment.
  */
trait ImpurityAggregator {
  def reset()
  def isEmpty(): Boolean
  def add(other: ImpurityAggregator)
  def sub(other: ImpurityAggregator)
  def getValue(): Double
  def getCount(): Int
  def splitValue(other: ImpurityAggregator, out: SplitImpurity): Double
}

/**
  * Mutable class that encapsulates classification impirity calculation.
  * The state is modified by adding or removing speficic labels.
  */
trait ClassificationImpurityAggregator extends ImpurityAggregator {
  def addLabel(label: Int)
  def subLabel(label: Int)
}

/**
  * Mutatabe class that encapsulates regression impority calculation.
  * The stat is modified by adding or removing continous values.
  */
trait RegressionImpurityAggregator extends ImpurityAggregator {
  def addValue(value: Double)
  def subValue(value: Double)
}

/**
  *  Base trait for representing impurituy measure
  */
trait Impurity

/**
  * Base trait for representing classification impurity measures.
  */
trait ClassficationImpurity extends Impurity {

  /**
    * Creates an aggregator for this impurity.
    *
    * @param nCategories the number of categories (lables) in the response variable.
    */
  def createAggregator(nCategories: Int): ClassificationImpurityAggregator
}
