package au.csiro.variantspark.genomics.reprod

import scala.collection.mutable.HashMap
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Buffer
import au.csiro.variantspark.genomics._

// I need some recombination pattern of the specific recombination chosen for 
// for each homozigote (from one parent each)
// So essentially for each chromosome I need to model how many and where the recombination happened

trait MutableVariant {
  def contig: ContigID
  def pos: Long
  def ref: BasesVariant
  def getOrElseUpdate(alt: BasesVariant):IndexedVariant
}

/**
 * List of cross-over points for a single contig (chromosome)
 * The list needs to be sorted (or will be sorted as needed)
 * for pos <  crossingOvers(0) -> chromosome 0
 * for crossingOvers(0) <= pos < crossingOvers(1) -> chromosome 1
 * for crossingOvers(1) <= pos < crossingOvers(2) -> chromosome 0 
 * etc ...
 * @param startWith: which chromosome in the pair to start with ( 0 or 1)
 */
case class MeiosisSpec(crossingOvers:List[Long], startWith:Int = 0) {
  
  def getChromosomeAt(pos:Long):Int  = {
    // the meiosis spec array should be sorted so essentially 
    // we just need to find out how may crossing points is before  this pos
    // 
    val noOfCrossOvers = crossingOvers.zipWithIndex.find(_._1 >  pos)
          .map(_._2).getOrElse(crossingOvers.length)
    // now an even number means first chromosome and an odd number the second
    (noOfCrossOvers + startWith)  % 2
  }
}

/**
 * Full specification a homozigote recombination patterns.
 * This can be now used to create the parental homozigore from the actual chromosome information
 */
case class GameteSpec(val splits:Map[ContigID, MeiosisSpec], val mutationSet:MutationSet =  MutationSet.Empty)  {
  assert(splits.isInstanceOf[Serializable])
  
  def homozigoteAt(v: MutableVariant, genotype: GenotypeSpec):Int = {
    // depending on the recombination pattersn return the allele from either the first 
    // or the second chromosome
    
    // check first for mutations
    // TODO: it appears that even thought the vcf is muliallelic
    // there are still possibilites for repeated positions with different references
    // e.g. one for SNPs and one for deletions
    
    
    mutationSet.get(GenomicCoord(v.contig,v.pos)).flatMap(m =>
      if (m.ref == v.ref) Some(v.getOrElseUpdate(m.alt)) else None
    ).getOrElse(genotype(splits(v.contig).getChromosomeAt(v.pos)))
  }
}

trait MeiosisSpecFactory {
  def createMeiosisSpec(): Map[ContigID, MeiosisSpec]
}

case class GameteSpecFactory(msf: MeiosisSpecFactory, mf: Option[MutationSetFactory] = None)  {
  //TODO: Add gender distintion
  def createGameteSpec():GameteSpec = GameteSpec(msf.createMeiosisSpec(),
        mf.map(_.create()).getOrElse(MutationSet.Empty))
}

case class OffspringSpec(val fatherGamete: GameteSpec, val motherGamete: GameteSpec) {
  
  def genotypeAt(v:MutableVariant, fatherGenotype:GenotypeSpec, motherGenotype:GenotypeSpec):GenotypeSpec = {
    GenotypeSpec(fatherGamete.homozigoteAt(v, fatherGenotype), motherGamete.homozigoteAt(v, motherGenotype))
  }
}

object OffspringSpec {  
  def create(gsf: GameteSpecFactory)  = OffspringSpec( 
      fatherGamete = gsf.createGameteSpec(),
      motherGamete = gsf.createGameteSpec()
  )
}




