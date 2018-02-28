package au.csiro.variantspark.hail.family

import is.hail.variant.GenericDataset
import au.csiro.variantspark.pedigree.MutationSetBatchFactory
import au.csiro.variantspark.pedigree.MutationSet
import is.hail.variant.Variant
import au.csiro.variantspark.pedigree.Mutation
import au.csiro.variantspark.pedigree.ContigSet
import au.csiro.pbdava.ssparkle.common.utils.Logging
import au.csiro.variantspark.utils.defRng
import it.unimi.dsi.util.XorShift1024StarRandomGenerator
import org.apache.spark.rdd.RDD


object DatasetMutationFactory {
  
  val bases = Set("C","T","G","A")
  
  def allSNP(v: Variant):Boolean  = {
    v.altAlleles.forall { x => x.isSNP }
  }
  
  def snpMutation(v: Variant): Iterator[Mutation]  = {
    // assume it's possible to do one
    // need to filter out the existing alleles 
    // select a random from available ones
    val usedSnps = v.altAlleles.filter(_.isSNP).map(_.alt).toSet + v.ref
    val mutationSnps =  bases.diff(usedSnps)
    assert(!mutationSnps.isEmpty, "Need a non empty mutation set")
    mutationSnps.iterator.map(Mutation(v, v.ref,_))
  }
}

/**
 * TODO: need to consider the mutation rate (which needs somehow to be converted to the probabilty of mutation
 * Also this will create non overlapping mutations all offspring (which may be ok)
 * 
 */
class DatasetMutationFactory(variantsRDD: RDD[Variant], 
    mutationRate: Double, contigSet:ContigSet, seed:Long =  defRng.nextLong) extends MutationSetBatchFactory with Logging {
  
  implicit private val rng = new XorShift1024StarRandomGenerator(seed)
  import DatasetMutationFactory._
  // in principle I could just generate a single stream and then split it into sets
  lazy val allMutations = variantsRDD
   .filter(allSNP)
   .filter(_.nAltAlleles <= 2) // at least one base to choose from (ref and two alts are used)
   .flatMap(snpMutation).cache()
   
  // these are all available mutations 
  lazy val mutationCount = allMutations.count()  
  
  override def create():MutationSet = {
    
    logInfo(s"Found ${mutationCount} mutation candidates")
    val samplingFraction:Double = mutationRate * contigSet.totalLenght / mutationCount
    logInfo(s"Samplig mutations with: ${samplingFraction} rate")
    
    if (samplingFraction > 1) {
      logWarning("There is not enough mutation canditates to draw ${mutationRate * contigSet.totalLenght} mutations")
    }
    MutationSet(allMutations.sample(false, samplingFraction, rng.nextLong()).collect()) 
  }
  
    // split mutations to sets
    // here or couild be done in parallel as well
    // zip with stream of randomm ints
    //allMutations.map(m => (m, (Math.random() * batchSize).toInt)).groupBy(_._2)
    //  .valuesIterator.map(a => MutationSet(a.unzip._1.toSeq)).toSeq
}