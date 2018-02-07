package au.csiro.variantspark.pedigree

import scala.collection.mutable.HashMap

trait FamilyMember extends Serializable {
  def id: IndividualID
}

case class Founder(val id:IndividualID) extends FamilyMember 

case class Offspring(val trio: FamilyTrio, val offspring: OffspringSpec) extends FamilyMember {
  def id =  trio.id
  def makeGenotype(position: GenomicPos, pool: GenotypePool):GenotypeSpec[Int] = {
    offspring.genotypeAt(position, pool(trio.paternalId.get), pool(trio.maternalId.get))
  }  
}

/**
 * Looks like a graf of Offspring Specs tied to the pedigree tree.
 */
class FamilySpec(val members:Seq[FamilyMember]) extends Serializable {

  def memberIds:List[IndividualID] =  members.map(_.id).toList
  
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

object FamilySpec {
  
  /**
   * Constuct Family spec from a Pedgree Tree and some form of Offspring Producer
   */
  
  def apply(pedigreeTree: PedigreeTree, gameteFactory: GameteSpecFactory):FamilySpec = {
    // get the trios in topological order and map them to FamilyMembers
    val familyMembers:Seq[FamilyMember] = pedigreeTree.orderedTrios.map{ trio =>
      if (trio.isFullOffspring) Offspring(trio, OffspringSpec.create(gameteFactory))
      else if (trio.isFounder) Founder(trio.id)
      else throw new IllegalArgumentException("PedigreeTree contatins incomplete trios")
    }
    new FamilySpec(familyMembers)
  }
}

