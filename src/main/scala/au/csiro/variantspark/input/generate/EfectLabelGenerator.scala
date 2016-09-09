package au.csiro.variantspark.input.generate

import au.csiro.variantspark.input.LabelSource
import au.csiro.variantspark.input.FeatureSource
import au.csiro.pbdava.ssparkle.common.utils.FastUtilConversions._
import au.csiro.variantspark.utils.VectorRDDFunction._
import org.apache.commons.math3.random.GaussianRandomGenerator
import org.apache.commons.math3.random.JDKRandomGenerator
import breeze.linalg.DenseVector
import org.apache.commons.math3.random.RandomGenerator
import it.unimi.dsi.util.XorShift1024StarRandomGenerator

class EfectLabelGenerator(featureSource:FeatureSource)(zeroLevel:Int, val noiseSigma:Double, 
      effects:Map[Long,Double], seed:Long = 13L) extends LabelSource {
  
  
  def logistic(d:Double) = 1.0 / (1.0 + Math.exp(-d))
  
  def getLabels(labels:Seq[String]):Array[Int] = {
    val nSamples = labels.size
    val rng = new XorShift1024StarRandomGenerator(seed)
    // this is actually very simple
    
    // generate continous variable with provided coefcients
    // then use logit transform to get class probabilties
    
    
    // ok so draw initial effect from a normal distribution
    // N(0,noiseSigma).
    // let's assume the number of incluential factors is small
    // so
    val influentialVariablesData = featureSource.features()
      .map(_.toVector.values).zipWithIndex().collectAtIndexes(effects.keySet)
    val gs = new GaussianRandomGenerator(rng)
    val globalNormalizer = DenseVector.fill(nSamples, zeroLevel.toDouble)
    val continousEffects = effects.foldLeft(DenseVector.fill[Double](nSamples)(gs.nextNormalizedDouble() *noiseSigma)) { case (a, (vi, e)) =>
      // in essence apply the effect function to the input data
      val additiveEffect = DenseVector(influentialVariablesData(vi).toArray) - globalNormalizer
      additiveEffect*=2*e
      a+=additiveEffect
    }
    continousEffects.map(c => if (logistic(c) >=rng.nextDouble()) 1 else 0).toArray
  }
  
}


object EfectLabelGenerator {
  def apply(featureSource:FeatureSource)(zeroLevel:Int, noiseSigma:Double, effects:Map[Long,Double], seed:Long = 13L) = new EfectLabelGenerator(featureSource)(zeroLevel, noiseSigma, effects, seed)
}
