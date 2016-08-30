package au.csiro.variantspark.metrics

import au.csiro.variantspark.utils.FactorVariable

object Gini {
  def sqr(x: Double) = x * x

  def giniImpurityWithTotal(counts: Array[Int]): (Double, Int) = {
    val total = counts.sum
    val totalAsDouble = total.toDouble
    if (total == 0) (0.0, total) else (1 - counts.map(s => sqr(s / totalAsDouble)).sum, total)
  }

  
  def giniImpurity(counts: Array[Int]): Double = giniImpurityWithTotal(counts)._1

  
  def splitGiniInpurity(leftCounts: Array[Int], totalCounts:Array[Int]) =  {
    val (leftGini, leftTotal) = giniImpurityWithTotal(leftCounts)
    val (rightGini, rightTotal) = giniImpurityWithTotal(totalCounts.zip(leftCounts).map(t => t._1 -t._2).toArray)
    (leftGini, rightGini, (leftGini* leftTotal + rightGini* rightTotal)/(leftTotal + rightTotal))
  }
  
  def giniImpurity(currentSet: Array[Int], labels: Array[Int], labelCount:Int):(Double,Int) = {
    val labelCounts = Array.fill(labelCount)(0)
    currentSet.foreach(i => labelCounts(labels(i)) += 1)
    (giniImpurity(labelCounts), labelCounts.zipWithIndex.max._2)
  }

  def giniImpurity(factor: FactorVariable):(Double,Int) = {
    val labelCounts = factor.counts 
    (giniImpurity(labelCounts), labelCounts.zipWithIndex.max._2)
  }
}