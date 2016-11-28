package au.csiro.variantspark.input.generate

import org.apache.commons.math3.random.GaussianRandomGenerator
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

import au.csiro.pbdava.ssparkle.spark.SparkUtils.withBroadcast
import au.csiro.variantspark.input.FeatureSource
import au.csiro.variantspark.input.LabelSource
import breeze.linalg.DenseVector
import it.unimi.dsi.util.XorShift1024StarRandomGenerator
import breeze.linalg.operators.DenseVector_GenericOps
import org.apache.spark.Logging
import breeze.stats.meanAndVariance
import breeze.stats.MeanAndVariance
import breeze.stats.DescriptiveStats

/**
 * Generate a dichotomous response 
 */
class NoisyEfectLabelGenerator(featureSource:FeatureSource)(zeroLevel:Int, 
      effects:Map[String,Double],  val fractionVarianceExplained:Double, val classThresholdPrecentile:Double = 0.75, seed:Long = 13L) extends LabelSource with Logging {
  
  def logistic(d:Double) = 1.0 / (1.0 + Math.exp(-d))
 
  lazy val rng = new XorShift1024StarRandomGenerator(seed) 
  
  
  // TODO: (Refactoring) make it a lazy vals
  var baseContinuousResponse:DenseVector[Double]  = null
  var baseContinousStats:MeanAndVariance = null
  var noisyContinuousResponse:DenseVector[Double]  = null
  var noisyContinousStats:MeanAndVariance = null
  
  def getLabels(labels:Seq[String]):Array[Int] = {

 
    val zeroLevelValue = zeroLevel.toDouble
    val nSamples = labels.size
    
    baseContinuousResponse = withBroadcast(featureSource.features.sparkContext)(effects){ br_effects =>
       featureSource.features.filter(f => br_effects.value.contains(f.label)).mapPartitions {it => 
         val normalizer = DenseVector.fill(nSamples)(zeroLevelValue)
         it.map(f => (DenseVector(f.toVector.values.toArray)-=normalizer) *= (br_effects.value(f.label)))
       }.fold(DenseVector.zeros[Double](nSamples))(_+=_)
    }
    
    logDebug(s"Continuous response: ${baseContinuousResponse}")
    
    baseContinousStats = meanAndVariance(baseContinuousResponse)

    logDebug(s"Continuous mav: ${baseContinousStats}")
    
    // based on the variablity of the base respone we can caculate the sigma of the desired noise level    
    val actualNoiseVar = baseContinousStats.variance * (1 - fractionVarianceExplained) / fractionVarianceExplained
    logDebug(s"Actual noise variance: ${actualNoiseVar}")
    
    val actualNoiseSigma = Math.sqrt(actualNoiseVar)    
    val noise = DenseVector.fill(nSamples)(rng.nextGaussian() * actualNoiseSigma)
    noisyContinuousResponse = baseContinuousResponse + noise
    
    noisyContinousStats  = meanAndVariance(noisyContinuousResponse)
    logDebug(s"Actual noisy response stats: ${noisyContinousStats}")
 
    val actualFractionVarianceExplained =  baseContinousStats.variance / noisyContinousStats.variance
    
    logDebug(s"Actual fraction variance explained: ${actualFractionVarianceExplained}")

    val classThreshold = DescriptiveStats.percentile(noisyContinuousResponse.valuesIterator, classThresholdPrecentile)
    logDebug(s"Actual class threshold: ${classThreshold}")
  
    val classes = noisyContinuousResponse.map(v => if (v > classThreshold) 1 else 0)

    logDebug(s"Classes: ${classes}")
    classes.toArray  
  }
}


object NoisyEfectLabelGenerator {
  def apply(featureSource:FeatureSource)(zeroLevel:Int, 
      effects:Map[String,Double], fractionVarianceExplained:Double, classThresholdPrecentile:Double = 0.75 , seed:Long = 13L) = new NoisyEfectLabelGenerator(featureSource)(zeroLevel, effects, fractionVarianceExplained, classThresholdPrecentile, seed)
}
