package au.csiro.variantspark.genomics.family

import scala.collection.mutable.HashMap
import scala.io.Source
import org.json4s._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.{read, writePretty}
import java.io.Writer
import au.csiro.variantspark.genomics._
import au.csiro.variantspark.genomics.reprod.GameteSpecFactory
import au.csiro.variantspark.genomics.reprod.MutableVariant
import au.csiro.variantspark.genomics.reprod.OffspringSpec
import au.csiro.variantspark.genomics.family.Gender._

trait FamilyMember extends Serializable {
  def id: IndividualID
  def gender: Gender
}

case class Founder(val id:IndividualID, val gender:Gender) extends FamilyMember {
  def this(trio: FamilyTrio) {
    this(trio.id, trio.gender)
    assert(trio.isFounder)
  }
}

case class Offspring(val id:IndividualID, val gender:Gender, val paternalId:IndividualID, maternalId:IndividualID, 
          val offspring: OffspringSpec) extends FamilyMember {
  
  def this(trio: FamilyTrio, offspring: OffspringSpec) {
    this(trio.id, trio.gender, trio.paternalId.get, trio.maternalId.get, offspring)
    assert(trio.isFullOffspring)
  }
  def makeGenotype(v: MutableVariant, pool: GenotypePool):GenotypeSpec = {
    offspring.genotypeAt(v, pool(paternalId), pool(maternalId))
  }  
}

/**
 * Looks like a graf of Offspring Specs tied to the pedigree tree.
 */
case class FamilySpec(val members:Seq[FamilyMember]) extends Serializable {
  
  def memberIds:List[IndividualID] =  members.map(_.id).toList
  def founderIds:List[IndividualID] = members.filter(p => p.isInstanceOf[Founder]).map(_.id).toList
  
  def offsprings: Seq[Offspring] = members.filter(p => p.isInstanceOf[Offspring]).map(_.asInstanceOf[Offspring])
  
  def produceGenotypePool(v: MutableVariant, initialPool:GenotypePool):GenotypePool = {
    
    val outputPool =  HashMap[IndividualID, GenotypeSpec]()
    members.foreach {  m =>  m match {
      case founder:Founder => outputPool.put(founder.id, initialPool(founder.id))
      case offspring: Offspring => outputPool.put(offspring.id, offspring.makeGenotype(v, outputPool))
      }   
    }
    outputPool.toMap
  }
  
  def toJson(w:Writer)  {
    implicit val formats = Serialization.formats(ShortTypeHints(List(classOf[Founder], 
        classOf[Offspring]))) + new org.json4s.ext.EnumNameSerializer(Gender)
    w.append(writePretty(this))
  }
  
  def summary: FamilySpec.Summary = {
    val noFounders  = members.filter(p => p.isInstanceOf[Founder]).toIterable.size;
    FamilySpec.Summary(noFounders, members.size - noFounders)
  }
}

object FamilySpec {
  case class Summary(val noFounders:Int, val noOffspring:Int) {
    def total = noFounders  + noOffspring
  }
  
  implicit val formats = Serialization.formats(ShortTypeHints(List(classOf[Founder], 
      classOf[Offspring]))) + new org.json4s.ext.EnumNameSerializer(Gender)
  
  /**
   * Constuct Family spec from a Pedgree Tree and some form of Offspring Producer
   */
  
  def apply(pedigreeTree: PedigreeTree, gameteFactory: GameteSpecFactory):FamilySpec = {
    // get the trios in topological order and map them to FamilyMembers
    val familyMembers:Seq[FamilyMember] = pedigreeTree.orderedTrios.map{ trio =>
      if (trio.isFullOffspring) new Offspring(trio, OffspringSpec.create(gameteFactory))
      else if (trio.isFounder) new Founder(trio)
      else throw new IllegalArgumentException("PedigreeTree contatins incomplete trios")
    }
    new FamilySpec(familyMembers)
  }
  
  def fromJson(src:Source):FamilySpec = {
    read[FamilySpec](src.mkString)
  }
}

