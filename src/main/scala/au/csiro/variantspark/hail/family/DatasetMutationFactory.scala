package au.csiro.variantspark.hail.family

import is.hail.variant.GenericDataset
import au.csiro.variantspark.pedigree.MutationSetBatchFactory
import au.csiro.variantspark.pedigree.MutationSet
import is.hail.variant.Variant
import au.csiro.variantspark.pedigree.Mutation

object DatasetMutationFactory {
  
  val bases = Set("C","T","G","A")
  
  def allSNP(v: Variant):Boolean  = {
    v.altAlleles.forall { x => x.isSNP }
  }
  
  def snpMutation(v: Variant): Mutation  = {
    // assume it's possible to do one
    // need to filter out the existing alleles 
    // select a random from available fones
    val altSnps = v.altAlleles.filter(_.isSNP).map(_.alt).toSet
    val mutationSnps =  bases.diff(altSnps)
    assert(!mutationSnps.isEmpty, "Need a non empty mutation set")
    //TODO: select a random mutation
    Mutation(v, v.ref, mutationSnps.iterator.next)
  }
}

/**
 * TODO: need to consider the mutation rate (which needs somehow to be converted to the probabilty of mutation
 * Also this will create non overlapping mutations all offspring (which may be ok)
 * 
 */
class DatasetMutationFactory(vgs: GenericDataset) extends MutationSetBatchFactory {
 
  def createBatch(batchSize: Int): Seq[MutationSet] = {
    
    import DatasetMutationFactory._
    // in principle I could just generate a single stream and then split it into sets
    val allMutations = vgs.rdd.map(_._1)
     .filter(allSNP)
     .filter(_.nAltAlleles <= 2) // at least  a few new variants to choose from
     .map(snpMutation).collect()
  
    // split mutations to sets
    // here or couild be done in parallel as well
    // zip with stream of randomm ints
    allMutations.map(m => (m, (Math.random() * batchSize).toInt)).groupBy(_._2)
      .valuesIterator.map(a => MutationSet(a.unzip._1.toSeq)).toSeq
  }
}