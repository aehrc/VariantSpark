package au.csiro.variantspark.pedigree

import scala.collection.mutable.HashMap

case class OffspringTrio(val trio: FamilyTrio, val offspring: OffspringSpec)  {
  def offspringID = trio.id
  def makeGenotype(position: GenomicPos, population: HashMap[IndividualID, GenotypeSpec[Int]]):GenotypeSpec[Int] = {
    offspring.genotypeAt(position, population(trio.maternalId.get), population(trio.paternalId.get))
  }
}

/**
 * Looks like a graf of Offspring Specs tied to the pedigree tree.
 */
class FamilySpec {

  def founders:List[IndividualID]  = ???
  def individuals:List[IndividualID] = ???
  def offspring: List[OffspringTrio] = ???
}