package au.csiro.variantspark.genomics

object Defaults {
  
  /**
   * ~1.1×10−8 per base pair per generation see:  (https://en.wikipedia.org/wiki/Mutation_rate)
   */
  val humanMutationRate:Double = 1.1e-8
}

case class GenomicCoord(val contig:ContigID, pos:Long)

/**
 * Represents a diplod indexed genotype
 * TODO: Rename to include diploid
 */
case class GenotypeSpec(val p:Int) extends AnyVal {
  def _0: Int = p & ((1 << 16) -1)
  def _1: Int = (p >> 16)
  def apply(index:Int): Int = if (index == 0) _0 else if (index == 1) _1 else throw new IllegalArgumentException()
}

object GenotypeSpec {
  def apply(p0:Int, p1: Int) = {
    assert(p0 >=0 && p0 < (1<<15) && p1 >=0 && p1 < (1<<15), s"p0=${p0}, p1=${p1}")
    new GenotypeSpec(p0 | (p1 << 16))
  }   
}