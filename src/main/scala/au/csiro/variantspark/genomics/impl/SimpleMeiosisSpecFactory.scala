package au.csiro.variantspark.genomics.impl

import au.csiro.variantspark.genomics._
import it.unimi.dsi.util.XorShift1024StarRandomGenerator
import au.csiro.variantspark.utils.defRng
import au.csiro.variantspark.genomics.reprod.MeiosisSpecFactory
import au.csiro.variantspark.genomics.reprod.MeiosisSpec


/**
 * Very basic factory that just generates once random crossing over 
 * in each contig
 */
case class SimpleMeiosisSpecFactory(val contigSet:ContigSet, seed:Long = defRng.nextLong) extends MeiosisSpecFactory {
  
  val rng = new XorShift1024StarRandomGenerator(seed)
  def createMeiosisSpec(): Map[ContigID, MeiosisSpec] = contigSet.contigs.map(cs => (cs.id, 
        MeiosisSpec(List(rng.nextLong(cs.length)), rng.nextInt(2)))).toMap
}
