package au.csiro.variantspark.input.generate

import org.apache.commons.math3.random.GaussianRandomGenerator
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

import au.csiro.pbdava.ssparkle.spark.SparkUtils.withBroadcast
import au.csiro.variantspark.input.LabelSource
import breeze.linalg.DenseVector
import it.unimi.dsi.util.XorShift1024StarRandomGenerator
import breeze.linalg.operators.DenseVector_GenericOps
import au.csiro.pbdava.ssparkle.common.utils.Logging
import breeze.stats.meanAndVariance
import breeze.stats.MeanAndVariance
import breeze.stats.DescriptiveStats
import org.apache.spark.rdd.RDD
import au.csiro.variantspark.input.FeatureSource

/**
  * Generate a dichotomous response
  */
class NoisyEffectLabelGenerator(featureSource: FeatureSource)(
    zeroLevel: Int,
    effects: Map[String, Double],
    val fractionVarianceExplained: Double,
    val classThresholdPercentile: Double = 0.75,
    val multiplicative: Boolean = false,
    seed: Long = 13L)
    extends LabelSource
    with Logging {

  def logistic(d: Double) = 1.0 / (1.0 + Math.exp(-d))

  lazy val rng = new XorShift1024StarRandomGenerator(seed)

  // TODO: (Refactoring) make it a lazy vals
  var baseContinuousResponse: DenseVector[Double] = _
  var baseContinuousStats: MeanAndVariance = _
  var noisyContinuousResponse: DenseVector[Double] = _
  var noisyContinuousStats: MeanAndVariance = _

  def foldAdditive(nSamples: Int, rdd: RDD[DenseVector[Double]]) =
    rdd.fold(DenseVector.zeros[Double](nSamples))(_ += _)
  def foldMulitiplicative(nSamples: Int, rdd: RDD[DenseVector[Double]]) = {
    rdd
      .map(v => v.map(d => if (d == 0.0) 1.0 else d))
      .fold(DenseVector.ones[Double](nSamples))(_ *= _)
  }

  def getLabels(labels: Seq[String]): Array[Int] = {

    val zeroLevelValue = zeroLevel.toDouble
    val nSamples = labels.size

    baseContinuousResponse = withBroadcast(featureSource.features.sparkContext)(effects) {
      br_effects =>
        val rdd =
          featureSource.features.filter(f => br_effects.value.contains(f.label)).mapPartitions {
            it =>
              val normalizer = DenseVector.fill(nSamples)(zeroLevelValue)
              it.map(
                f =>
                  (DenseVector(f.valueAsVector.toArray) -= normalizer) *= (br_effects.value(
                    f.label)))
          }
        if (multiplicative) foldMulitiplicative(nSamples, rdd) else foldAdditive(nSamples, rdd)
    }

    logDebug(s"Continuous response: ${baseContinuousResponse}")

    baseContinuousStats = meanAndVariance(baseContinuousResponse)

    logDebug(s"Continuous mav: ${baseContinuousStats}")

    // based on the variability of the base response we can calculate the sigma of the desired noise level
    val actualNoiseVar =
      baseContinuousStats.variance * (1 - fractionVarianceExplained) / fractionVarianceExplained
    logDebug(
      s"Signal varinace: ${baseContinuousStats.variance}, fractio to explain: ${fractionVarianceExplained}")
    logDebug(s"Actual noise variance: ${actualNoiseVar}")

    val actualNoiseSigma = Math.sqrt(actualNoiseVar)
    val noise = DenseVector.fill(nSamples)(rng.nextGaussian() * actualNoiseSigma)
    logDebug(s"Noise: ${noise}")
    noisyContinuousResponse = baseContinuousResponse + noise
    logDebug(s"noisyContinuousResponse: ${noisyContinuousResponse}")

    noisyContinuousStats = meanAndVariance(noisyContinuousResponse)
    logDebug(s"Actual noisy response stats: ${noisyContinuousStats}")

    val actualFractionVarianceExplained =
      baseContinuousStats.variance / noisyContinuousStats.variance

    logDebug(s"Actual fraction variance explained: ${actualFractionVarianceExplained}")

    val classThreshold = DescriptiveStats.percentile(
      noisyContinuousResponse.valuesIterator,
      classThresholdPercentile)
    logDebug(s"Actual class threshold: ${classThreshold}")

    val classes = noisyContinuousResponse.map(v => if (v > classThreshold) 1 else 0)

    logDebug(s"Classes: ${classes}")
    classes.toArray
  }
}

object NoisyEffectLabelGenerator {
  def apply(featureSource: FeatureSource)(
      zeroLevel: Int,
      effects: Map[String, Double],
      fractionVarianceExplained: Double,
      classThresholdPercentile: Double = 0.75,
      multiplicative: Boolean = false,
      seed: Long = 13L) =
    new NoisyEffectLabelGenerator(featureSource)(
      zeroLevel,
      effects,
      fractionVarianceExplained,
      classThresholdPercentile,
      multiplicative,
      seed)
}
