package au.csiro.variantspark.genomics.impl

import au.csiro.variantspark.genomics.reprod.MutationSetFactory
import au.csiro.variantspark.genomics.reprod.MutationSet
import au.csiro.variantspark.genomics.GenomicCoord
import au.csiro.variantspark.genomics.DNABase
import au.csiro.variantspark.genomics.ContigSet
import au.csiro.variantspark.genomics.Defaults
import au.csiro.variantspark.utils.Sampling
import org.apache.commons.math3.random.RandomGenerator
import au.csiro.variantspark.genomics.reprod.Mutation
import org.apache.commons.math3.distribution.PoissonDistribution


trait ReferenceSequence {
  
  /**
   * Get get reference base at given position
   */
  def apply(pos:GenomicCoord):DNABase 
  def contigSet: ContigSet
}

/**
 * This represents a mutation factory based on the actual reference genome/seuence
 * I a not not sure how this is actually represented, so this is more an exercise
 * in understanding the mutation process
 * 
 * ? do mutations affect only one chromosome of both of then independently?
 * ? what is the homo/hetero rate in fake family
 * ? simplified model with no spectrum or hotspots (the same probability of each mutation)
 * 
 * ? what if a mutation happens on a existing position (or precisely is existing variant)
 *  -- this will will only be an issue with fasta based approach but for now perhaps just ignore
 *  -- otherwise mutation couild be only generate post factum
 *  -- but this should be OK for variant based model
 *  
 *  @param mutationRate: mutation rate [base pairs per generation]
 */
class BamMutationFactory(val refSeq: ReferenceSequence, 
    val mutationRate:Double = Defaults.humanMutationRate)(implicit val rng: RandomGenerator) extends MutationSetFactory {
  /**
   * So: I need to know how many mutations to create possibly for each contig.
   * Also in the current approach mutation in each of the chromosomes happen independently.
   * TODO: is this OK?
   */
  def create(): MutationSet = {
    // so that would  be for each contig independently draw mutation points 
    // based on the mutation rate  (need a function to sample without replacement from k out of n)
    val mutations = refSeq.contigSet.toSeq.flatMap { contigSpec => 
      val poissonDist = new PoissonDistribution(contigSpec.length * mutationRate)
      //TODO: how to plug in my rng
      val noOfMutations = poissonDist.sample()
      val mutationPositions = Sampling.subsample(noOfMutations, contigSpec.length.toInt, false)
      mutationPositions.map(GenomicCoord.apply(contigSpec.id,_))
        .map(gp => Mutation.makeRandom(gp, refSeq(gp)))
    }
    MutationSet(mutations)
  }
}