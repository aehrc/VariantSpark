package au.csiro.variantspark.input.generate

import au.csiro.variantspark.input.FeatureSource
import org.apache.spark.rdd.RDD
import au.csiro.variantspark.input.ByteArrayFeature
import org.apache.spark.SparkContext
import au.csiro.variantspark.utils.Sampling
import it.unimi.dsi.util.XorShift1024StarRandomGenerator
import au.csiro.variantspark.input.Feature
import au.csiro.variantspark.input.CanRepresent
import au.csiro.variantspark.input._

case class OrdinalFeatureGenerator(val nLevels:Int, val nVariables:Long, 
      val nSamples:Int, val seed: Long = 13L, val sparkPar:Int = 0)(implicit sc:SparkContext) extends FeatureSource {
    
  def featuresAs[V](implicit cr:CanRepresent[V]=CanRepresentByteArray):RDD[Feature[V]] = {
    val nLevels = this.nLevels
    val nSamples = this.nSamples
    val seed = this.seed
    //TODO (Feature): Honor parallelism
    sc.range(0L, nVariables, numSlices = if (sparkPar > 0) sparkPar else sc.defaultParallelism)
      .mapPartitionsWithIndex { case (pi, iter) =>
        implicit val rf = new XorShift1024StarRandomGenerator(pi ^ seed)
        iter.map(i => cr.from("v_" + i, Sampling.subsample(nLevels, nSamples, true).map(_.toByte)))
      }
  }
  def sampleNames: List[String] =  Range(0,nSamples).map("s_"+_).toList 
  
}
