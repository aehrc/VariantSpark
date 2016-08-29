package au.csiro.variantspark.input.generate

import au.csiro.variantspark.input.LabelSource
import au.csiro.variantspark.input.FeatureSource
import au.csiro.pbdava.ssparkle.common.utils.FastUtilConversions._
import au.csiro.variantspark.utils.VectorRDDFunction._
import org.apache.commons.math3.random.GaussianRandomGenerator
import org.apache.commons.math3.random.JDKRandomGenerator
import breeze.linalg.DenseVector

case class EfectLabelGenerator(val noiseSigma:Double, 
      effects:Map[Long,Double], featureSource:FeatureSource
    ) extends LabelSource {
  
  def getLabels(labels:Seq[String]):Array[Int] = {
    val nSamples = labels.size
    // ok so draw initial effect from a normal distribution
    // N(0,noiseSigma).
    // let's assume the number of incluential factors is small
    // so
    val influentialVariablesData = featureSource.features()
      .map(_.toVector.values).zipWithIndex().collectAtIndexes(effects.keySet)
    val gs = new GaussianRandomGenerator(new JDKRandomGenerator())
    val globalNormalizer = DenseVector.fill(nSamples, 0.0)
    val continousEffects = effects.foldLeft(DenseVector.fill[Double](nSamples)(gs.nextNormalizedDouble() *noiseSigma)) { case (a, (vi, e)) =>
      // in essence apply the effect function to the input data
      val additiveEffect = DenseVector(influentialVariablesData(vi).toArray) - globalNormalizer
      additiveEffect*=2*e
      a+=additiveEffect
      a
    }
    continousEffects.map(c => if (c >1) 1 else 0).toArray 
  }
  
}

