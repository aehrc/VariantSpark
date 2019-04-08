package au.csiro.variantspark.input.generate

import org.apache.commons.math3.random.GaussianRandomGenerator
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

import au.csiro.pbdava.ssparkle.spark.SparkUtils.withBroadcast
import au.csiro.variantspark.input.FeatureSource
import au.csiro.variantspark.input.LabelSource
import breeze.linalg.DenseVector
import it.unimi.dsi.util.XorShift1024StarRandomGenerator
import breeze.linalg.operators.DenseVector_GenericOps
import au.csiro.pbdava.ssparkle.common.utils.Logging
import breeze.stats.meanAndVariance
import breeze.stats.MeanAndVariance

/**
 * Generate a dichotomous response 
 */
class EffectLabelGenerator(featureSource:FeatureSource)(zeroLevel:Int,
                                                        effects:Map[String,Double], val noiseEffectSigma:Double, val noiseEffectMean:Double= 0.1,
                                                        val noiseVarFraction:Double=0.0, seed:Long = 13L) extends LabelSource with Logging {

  def logistic(d:Double) = 1.0 / (1.0 + Math.exp(-d))



  lazy val rng = new XorShift1024StarRandomGenerator(seed)

  lazy val noiseEffects:Map[String, Double] =  {
    // select noise variables
    val gs = new GaussianRandomGenerator(rng)
    val noiseVariables:List[String] = if ( noiseVarFraction > 0.0) {
      withBroadcast(featureSource.features.sparkContext)(effects){ br_effects =>
        featureSource.features.filter(f => !br_effects.value.contains(f.label)).sample(false,noiseVarFraction, seed).map(_.label).collect().toList
      }
    } else {
      List.empty
    }
    // generate noise effect
    noiseVariables.zip(Stream.fill(noiseVariables.size)(gs.nextNormalizedDouble()*noiseEffectSigma + noiseEffectMean)).toMap
  }

  // ASSUMPTION: This is assuming independence and uniform distribution of the variables (with 0, 1, 2 and medium level 1)
  // TODO: Generalise
  val noiseMean =  noiseEffectMean
  lazy val noiseSigma = {
    println(s"Noise effect size: ${noiseEffects.size}")
    val r = Math.sqrt( noiseEffects.size * (2.0/3.0) * (2.0*noiseEffectSigma) * (2.0*noiseEffectSigma))
    println(s"R: ${r}")
    r
  }

  // TODO: (Refactoring) make it a lazy vals
  var continuousStats:MeanAndVariance = _
  var continouusResponse:DenseVector[Double] = _

  def getLabels(labels:Seq[String]):Array[Int] = {
    val nSamples = labels.size

    val allEffects =  effects ++ noiseEffects
    logDebug(s"Variable effects: ${effects}")
    logDebug(s"Noise effects: ${noiseEffects}")

    val zeroLevelValue = zeroLevel.toDouble
    continouusResponse = withBroadcast(featureSource.features.sparkContext)(allEffects){ br_effects =>
       featureSource.features.filter(f => br_effects.value.contains(f.label)).mapPartitions {it =>
         val normalizer = DenseVector.fill(nSamples)(zeroLevelValue)
         it.map(f => (DenseVector(f.valueAsVector.toArray)-=normalizer) *= (2 * br_effects.value(f.label)))
       }.fold(DenseVector.zeros[Double](nSamples))(_+=_)
    }

    continuousStats = meanAndVariance(continouusResponse)
    logDebug(s"Continuous mav: ${continuousStats}")
    logDebug(s"Continuous response: ${continouusResponse}")

    val classProbabilities = continouusResponse.map(logistic)

    logDebug(s"Class probabilities: ${classProbabilities}")

    val classes = classProbabilities.map(c => if (rng.nextDouble() < c) 1 else 0)

    logDebug(s"Classes: ${classes}")

    // print out correlation of variables
    //val output = classes.toArray.map(_.toDouble)
    //val correlationCalc = new PearsonsCorrelation()
    //effects.map { case (v,e) => (v, correlationCalc.correlation(output, influentialVariablesData(v).toArray)) }.foreach(println)
    classes.toArray
  }
}


object EffectLabelGenerator {
  def apply(featureSource:FeatureSource)(zeroLevel:Int,
      effects:Map[String,Double], noiseEffectSigma:Double = 0.0, noiseEffectMean:Double= 0.0,
       noiseVarFraction:Double=0.0 , seed:Long = 13L) = new EffectLabelGenerator(featureSource)(zeroLevel, effects, noiseEffectSigma, noiseEffectMean, noiseVarFraction, seed)
}
