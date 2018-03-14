package au.csiro.variantspark.hail.family

import is.hail.variant.GenericDataset
import au.csiro.variantspark.genomics.reprod.MutationSet
import is.hail.variant.Variant
import au.csiro.variantspark.genomics.reprod.Mutation
import au.csiro.variantspark.genomics.ContigSet
import au.csiro.pbdava.ssparkle.common.utils.Logging
import au.csiro.variantspark.utils.defRng
import it.unimi.dsi.util.XorShift1024StarRandomGenerator
import org.apache.spark.rdd.RDD
import au.csiro.variantspark.genomics.reprod.MutationSetFactory
import org.apache.commons.math3.random.RandomGenerator

object DatasetMutationFactory {
    
  def allSNP(v: Variant):Boolean  = {
    v.altAlleles.forall { x => x.isSNP }
  }
  
  def snpMutation(v: Variant): Iterator[Mutation]  = {
    Mutation.makeAll(v, v.ref, v.altAlleles.filter(_.isSNP).map(_.alt).toSet)
  }
  
  def toMutations(variantsRDD: RDD[Variant]):RDD[Mutation] = variantsRDD
     .filter(allSNP)
     .filter(_.nAltAlleles <= 2) // at least one base to choose from (ref and two alts are used)
     .flatMap(snpMutation)
    
  def apply(variantsRDD: RDD[Variant],mutationRate: Double, contigSet:ContigSet)
          (implicit rng:RandomGenerator):DatasetMutationFactory = {
          new DatasetMutationFactory(toMutations(variantsRDD), mutationRate, contigSet)
  }

  def apply(variantsRDD: RDD[Variant], 
    mutationRate: Double, contigSet:ContigSet, seed:Long =  defRng.nextLong):DatasetMutationFactory = {
    implicit  val rng = new XorShift1024StarRandomGenerator(seed)
    DatasetMutationFactory(variantsRDD, mutationRate, contigSet)
  }
}

/**
 * TODO: need to consider the mutation rate (which needs somehow to be converted to the probabilty of mutation
 * Also this will create non overlapping mutations all offspring (which may be ok)
 * 
 */
class DatasetMutationFactory(val mutations: RDD[Mutation], 
    val mutationRate: Double, val contigSet:ContigSet)(implicit rng:RandomGenerator) extends MutationSetFactory with Logging {
  
  lazy val mutationCount = mutations.cache().count()
  
  override def create():MutationSet = {
    
    logInfo(s"Found ${mutationCount} mutation candidates")
    val samplingFraction:Double = mutationRate * contigSet.totalLenght / mutationCount
    logInfo(s"Samplig mutations with: ${samplingFraction} rate")
    
    if (samplingFraction > 1) {
      logWarning("There is not enough mutation canditates to draw ${mutationRate * contigSet.totalLenght} mutations")
    }
    MutationSet(mutations.sample(false, samplingFraction, rng.nextLong()).collect()) 
  }
  
}