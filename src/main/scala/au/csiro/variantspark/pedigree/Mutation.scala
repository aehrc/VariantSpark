package au.csiro.variantspark.pedigree


/** 
 * Somehow need to be able to specify de-novo mutations.
 * At the moment only SNPs.
 * Access to the base genome currenty using VCF file (so only exising SNP positions).
 * Also try to only generate new variants (that is do not use existing SNPs but only new ones)
 * (So this limits the bases to choose for for mutations)
 */


case class Mutation(pos: GenomicPos, ref: String, alt: String)
case class MutationSet(mutations: Seq[Mutation])

trait MutationSetFactory {
  def create(): MutationSet
}


trait MutationSetBatchFactory extends MutationSetFactory {
  def createBatch(batchSize: Int): Seq[MutationSet]
  def create() = createBatch(0).head
}