package au.csiro.variantspark.pedigree

import scala.collection.mutable.HashMap

trait FamilyMember extends Serializable {
  def id: IndividualID
}

case class Founder(val id:IndividualID) extends FamilyMember 

case class Offspring(val trio: FamilyTrio, val offspring: OffspringSpec) extends FamilyMember {
  def id =  trio.id
  def makeGenotype(position: GenomicPos, pool: GenotypePool):GenotypeSpec[Int] = {
    offspring.genotypeAt(position, pool(trio.maternalId.get), pool(trio.paternalId.get))
  }  
}

/**
 * Looks like a graf of Offspring Specs tied to the pedigree tree.
 */
class FamilySpec(val members:List[FamilyMember]) extends Serializable {

  def memberIds:List[IndividualID] =  members.map(_.id)
  
  def produceGenotypePool(position: GenomicPos, initialPool:GenotypePool):GenotypePool = {
    
    val outputPool =  HashMap[IndividualID, IndexedGenotypeSpec]()
    members.foreach {  m =>  m match {
      case founder:Founder => outputPool.put(founder.id, initialPool(founder.id))
      case offspring: Offspring => outputPool.put(offspring.id, offspring.makeGenotype(position, outputPool.toMap))
      }   
    }
    outputPool.toMap
  }
}