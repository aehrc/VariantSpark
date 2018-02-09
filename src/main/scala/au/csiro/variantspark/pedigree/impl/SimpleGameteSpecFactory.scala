package au.csiro.variantspark.pedigree.impl

import au.csiro.variantspark.pedigree._
import it.unimi.dsi.util.XorShift1024StarRandomGenerator
import au.csiro.variantspark.utils.defRng


/**
 * Very basic factory that just generates once random crossing over 
 * in each contig
 */
case class SimpleGameteSpecFactory(val contigSet:ContigSet, seed:Long = defRng.nextLong) extends GameteSpecFactory {
  
  val rng = new XorShift1024StarRandomGenerator(seed)
  def createHomozigoteSpec(): GameteSpec = { 
    GameteSpec(contigSet.contigs.map(cs => (cs.id, MeiosisSpec(List(rng.nextLong(cs.length))))).toMap)  
  }
}
