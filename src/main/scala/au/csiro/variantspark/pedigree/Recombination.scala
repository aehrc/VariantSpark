package au.csiro.variantspark.pedigree

// I need some recombination pattern of the specific recombination chosen for 
// for each homozigote (from one parent each)
// So essentially for each chromosome I need to model how many and where the recombination happened


class GenotypeSpec[A](val alleles:List[A]) {
}

case class GenomicPos(val contig:ContigID, pos:Long)

/**
 * List of crossing over points for a single contig (chromosome)
 */
case class RecombinationSpec(crossingOvers:List[Long])

/**
 * Full specification a homozigote recombination patterns.
 * This can be now used to create the parental homozigore from the actual chromosome information
 */
case class HomozigoteSpec(val splits:Map[ContigID, RecombinationSpec])  {
  
  def homozigoteAt[A](position: GenomicPos, genotype: GenotypeSpec[A]):A = {
    // depending on the recombination pattersn return the allele from either the first 
    // or the second chromosome
    return genotype.alleles(0)
  }
}

case class OffspringSpec(val motherZigote: HomozigoteSpec, val fatherZigote: HomozigoteSpec) {
  
  def genotypeAt[A](position:GenomicPos, motherGenotype:GenotypeSpec[A], fatherGenotype:GenotypeSpec[A]):GenotypeSpec[A] = {
    new GenotypeSpec[A](List(motherZigote.homozigoteAt(position, motherGenotype), 
        fatherZigote.homozigoteAt(position, fatherGenotype)))
  }
}







