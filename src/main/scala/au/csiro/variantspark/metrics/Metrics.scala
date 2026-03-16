package au.csiro.variantspark.metrics

object Metrics {

  def over2(n: Int): Int = n * (n - 1) / 2

  def adjustedRandIndex(c1: List[Int], c2: List[Int]): Double = {
    require(c1.size == c2.size)
    val c1PartitionNo = c1.max + 1
    val c2PartitionNo = c2.max + 1
    val n = c1.size
    val contingencyMatrix = Array.fill(c1PartitionNo)(Array.fill(c2PartitionNo)(0))
    c1.indices.foreach { i => contingencyMatrix(c1(i))(c2(i)) += 1 }
    val c1Bound = contingencyMatrix.map(_.sum)
    val c2Bound = Range(0, c2PartitionNo).map(j => contingencyMatrix.map(_(j)).sum)
    val index: Int = contingencyMatrix.flatMap(_.map(over2)).sum
    val sumAi = c1Bound.map(over2).sum
    val sumBi = c2Bound.map(over2).sum
    (index.toDouble - sumAi * sumBi / over2(n).toDouble) /
      ((sumAi + sumBi) / 2.0 - sumAi * sumBi / over2(n).toDouble)
  }

  def accuracy(expected: Array[Int], predicted: Array[Int]): Double = {
    expected.toSeq.zip(predicted).count(i => i._1 != i._2).toDouble / expected.length
  }

  def classificationError(expected: Array[Int], predicted: Array[Int]): Double =
    accuracy(expected, predicted)

  def meanSquaredError(expected: Array[Double], predicted: Array[Double]): Double = {
    require(expected.length == predicted.length)
    expected.zip(predicted).map { case (e, p) => math.pow(e - p, 2) }.sum / expected.length
  }

  def rootMeanSquaredError(expected: Array[Double], predicted: Array[Double]): Double =
    math.sqrt(meanSquaredError(expected, predicted))

  def meanAbsoluteError(expected: Array[Double], predicted: Array[Double]): Double = {
    require(expected.length == predicted.length)
    expected.zip(predicted).map { case (e, p) => math.abs(e - p) }.sum / expected.length
  }

  def r2(expected: Array[Double], predicted: Array[Double]): Double = {
    require(expected.length == predicted.length)
    val meanExpected = expected.sum / expected.length
    val ssTot = expected.map(e => math.pow(e - meanExpected, 2)).sum
    val ssRes = expected.zip(predicted).map { case (e, p) => math.pow(e - p, 2) }.sum
    if (ssTot == 0) 0.0 else 1 - (ssRes / ssTot)
  }
}
