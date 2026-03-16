package au.csiro.variantspark.metrics

object Variance {
  def sqr(x: Double): Double = x * x

  def varianceImpurity(count: Int, sumOfValues: Double, sumOfSquares: Double): Double = {
    if (count == 0) 0.0
    else (sumOfSquares / count) - sqr(sumOfValues / count)
  }

  def varianceImpurity(indices: Array[Int],
      values: Array[Double]): (Double, Int, Double, Double) = {
    var sum = 0.0
    var sumSq = 0.0
    var i = 0
    val len = indices.length
    while (i < len) {
      val v = values(indices(i))
      sum += v
      sumSq += sqr(v)
      i += 1
    }
    (varianceImpurity(len, sum, sumSq), len, sum, sumSq)
  }
}

// TODO: Evalute two pass variance, Welford's method, and parallel variance calculation
// for efficiency and numerical stability.
// See https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance
