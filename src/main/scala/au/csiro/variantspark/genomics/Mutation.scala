package au.csiro.variantspark.genomics

import org.apache.commons.math3.random.RandomGenerator


/** 
 * Somehow need to be able to specify de-novo mutations.
 * At the moment only SNPs.
 * Access to the base genome currenty using VCF file (so only exising SNP positions).
 * Also try to only generate new variants (that is do not use existing SNPs but only new ones)
 * (So this limits the bases to choose for for mutations)
 * 
 * BTW: The estimated mutation rate in humans is ~1.1×10−8 per base pair per generation
 * see:  (https://en.wikipedia.org/wiki/Mutation_rate)
 */

case class Mutation(coord: GenomicCoord, ref: String, alt: String)

object Mutation {
  def makeRandom(pos: GenomicCoord, ref: DNABase)(implicit rng: RandomGenerator): Mutation = {
    //TODO: add actual geneation of mutation
    Mutation(pos, ref, ref)
  }
}

case class MutationSet(mutations: Seq[Mutation]) {
  private lazy val map = mutations.map(m => (m.coord, m)).toMap
  def get(pos: GenomicCoord):Option[Mutation] = map.get(pos)
}

object MutationSet {
  val Empty =  MutationSet(Seq.empty)
}

trait MutationSetFactory {
  def create(): MutationSet
}

trait MutationSetBatchFactory extends MutationSetFactory {
  //def createBatch(batchSize: Int): Seq[MutationSet]
  //def create() = createBatch(0).head
}