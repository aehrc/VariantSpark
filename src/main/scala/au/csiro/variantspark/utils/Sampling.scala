package au.csiro.variantspark.utils

import org.apache.commons.math3.random.{RandomDataGenerator, RandomGenerator}

/**
  * Represents a sample (possibly with replacement0 from an array of size `nSize` by storing the indexes of the elements
  * beloning to the sample.
  */
class Sample(val nSize: Int, val indexes: Array[Int]) {
  def asWeights: Array[Int] = ???
  def distinctIndexesIn: Set[Int] = indexes.toSet
  def distinctIndexesOut: Set[Int] = Range(0, nSize).toSet.diff(distinctIndexesIn)

  /**
    * The length of the sample
    */
  def length = indexes.length
}

object Sample {
  def all(nSize: Int) = new Sample(nSize, Range(0, nSize).toArray)
  def fraction(nSize: Int, fraction: Double, withReplacement: Boolean = false)(
      implicit rng: RandomGenerator) =
    new Sample(nSize, Sampling.subsampleFraction(nSize, fraction, withReplacement))
}

object Sampling {

  def subsample(size: Int, sampleSize: Int, withReplacement: Boolean)(
      implicit rg: RandomGenerator): Array[Int] = {
    if (!withReplacement && sampleSize > size)
      throw new RuntimeException("Sample size greater then sample len")
    val rdg = new RandomDataGenerator(rg)
    return if (withReplacement)
      Array.fill[Int](sampleSize)(rdg.nextInt(0, size - 1))
    else rdg.nextPermutation(size, sampleSize)
  }

  def subsample(size: Int, sampleSize: Int)(implicit rg: RandomGenerator): Array[Int] =
    subsample(size, sampleSize, false)

  def subsampleFraction(size: Int, fraction: Double, withReplacement: Boolean = false)(
      implicit rg: RandomGenerator): Array[Int] =
    subsample(size, if (fraction == 1.0) size else math.round(size * fraction).toInt,
      withReplacement)

  def folds(size: Int, nFolds: Int): List[Array[Int]] = {
    val rdg = new RandomDataGenerator()

    if (size < nFolds)
      throw new RuntimeException("Not enough elements, must be at least one per fold")
    val permutation = rdg.nextPermutation(size, size)
    val byFold = Array.range(0, size).groupBy(permutation(_) % nFolds)

    byFold.values.toList
  }

}
