package au.csiro.variantspark.input.generate

import au.csiro.variantspark.input.FeatureSource
import org.apache.spark.rdd.RDD
import au.csiro.variantspark.input.Feature
import org.apache.spark.SparkContext
import au.csiro.variantspark.utils.Sampling
import it.unimi.dsi.util.XorShiftStarRandomGenerator

case class OrdinalFeatureSource(val nLevels:Int, val nVariables:Int, 
      val nSamples:Int, val seed: Long = 13L)(implicit sc:SparkContext) extends FeatureSource {
    
  def features(): RDD[Feature]  = {
    val nLevels = this.nLevels
    val nSamples = this.nSamples
    val seed = this.seed
    sc.parallelize(Range(0, nVariables).toList)
      .mapPartitionsWithIndex { case (pi, iter) =>
        implicit val rf = new XorShiftStarRandomGenerator(pi ^ seed)
        iter.map(i => Feature("v_" + i, Sampling.subsample(nLevels, nSamples, true)))
      }
  }
  def sampleNames: List[String] =  Range(0,nSamples).map("s_"+_).toList 
  
}
