package au.csiro.variantspark.pedigree

// I need some recombination pattern of the specific recombination chosen for 
// for each homozigote (from one parent each)
// So essentially for each chromosome I need to model how many and where the recombination happened


trait GenotypeSpec[A] {
  type Factory = (A,A) => GenotypeSpec[A]
  def apply(index:Int): A
  def buildNew(a:A, b:A):GenotypeSpec[A]
}


case class IndexedBiGenotypeSpec(val g0:Int, val g1:Int) extends GenotypeSpec[Int] {
  def apply(index:Int): Int = if (index == 0) g0 else if (index == 1) g1 else throw new IllegalArgumentException()
  def buildNew(g0:Int, g1:Int):IndexedBiGenotypeSpec = IndexedBiGenotypeSpec(g0,g1)
}

case class GenomicPos(val contig:ContigID, pos:Long)

/**
 * List of cross-over points for a single contig (chromosome)
 * The list needs to be sorted (or will be sorted as needed)
 * for pos <  crossingOvers(0) -> chromosome 0
 * for crossingOvers(0) <= pos < crossingOvers(1) -> chromosome 1
 * for crossingOvers(1) <= pos < crossingOvers(2) -> chromosome 0 
 * etc ...
 */
case class MeiosisSpec(crossingOvers:Array[Long]) {
  
  def getChromosomeAt(pos:Long):Int  = {
    // the meiosis spec array should be sorted so essentially 
    // we just need to find out how may crossing points is before  this pos
    // 
    val noOfCrossOvers = crossingOvers.zipWithIndex.find(_._1 >  pos)
          .map(_._2).getOrElse(crossingOvers.length)
    // now an even number means first chromosome and an odd number the second
    noOfCrossOvers % 2
  }
}

/**
 * Full specification a homozigote recombination patterns.
 * This can be now used to create the parental homozigore from the actual chromosome information
 */
case class HomozigoteSpec(val splits:Map[ContigID, MeiosisSpec])  {
  
  def homozigoteAt[A](position: GenomicPos, genotype: GenotypeSpec[A]):A = {
    // depending on the recombination pattersn return the allele from either the first 
    // or the second chromosome
    
    //TODO: what to do if the contig is not listed
    val meiosisSpec = splits(position.contig)
    // the meiosis spec array should be sorted
    val chrosomeIndex = meiosisSpec.getChromosomeAt(position.pos)
    return genotype(chrosomeIndex)
  }
}

case class OffspringSpec(val motherZigote: HomozigoteSpec, val fatherZigote: HomozigoteSpec) {
  
  def genotypeAt[A](position:GenomicPos, motherGenotype:GenotypeSpec[A], fatherGenotype:GenotypeSpec[A]):GenotypeSpec[A] = {
    motherGenotype.buildNew(motherZigote.homozigoteAt(position, motherGenotype), 
        fatherZigote.homozigoteAt(position, fatherGenotype))
  }
}







