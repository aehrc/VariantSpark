package au.csiro.variantspark.metrics

import au.csiro.variantspark.utils.{ArraysUtils, FactorVariable}

object Gini {
  def sqr(x: Double): Double = x * x

  def giniImpurityWithTotal(counts: Array[Int]): (Double, Int) = {
    if (counts.length == 2) {
      val c1 = counts(0)
      val c2 = counts(1)
      val total = c1 + c2
      val p1 = c1.toDouble / total
      val p2 = c2.toDouble / total
      if (total == 0) (0.0, total) else (1.0 - p1 * p1 - p2 * p2, total)
    } else {
      val total = counts.sum
      val totalAsDouble = total.toDouble
      if (total == 0) (0.0, total) else (1 - counts.map(s => sqr(s / totalAsDouble)).sum, total)
    }
  }

  def giniImpurity(counts: Array[Int]): Double = giniImpurityWithTotal(counts)._1

  def splitGiniInpurity(leftCounts: Array[Int],
      totalCounts: Array[Int]): (Double, Double, Double) = {
    val (leftGini, leftTotal) = giniImpurityWithTotal(leftCounts)
    val (rightGini, rightTotal) =
      giniImpurityWithTotal(totalCounts.zip(leftCounts).map(t => t._1 - t._2))
    (leftGini, rightGini,
      (leftGini * leftTotal + rightGini * rightTotal) / (leftTotal + rightTotal))
  }

  def giniImpurity(currentSet: Array[Int], labels: Array[Int],
      labelCount: Int): (Double, Array[Int]) = {
    val labelCounts = Array.fill(labelCount)(0)
    currentSet.foreach(i => labelCounts(labels(i)) += 1)
    (giniImpurity(labelCounts), labelCounts)
  }

  def giniImpurity(factor: FactorVariable): (Double, Int) = {
    val labelCounts = factor.counts
    (giniImpurity(labelCounts), ArraysUtils.maxIndex(labelCounts))
  }
}
