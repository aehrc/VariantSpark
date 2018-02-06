package au.csiro.variantspark.hail.family

import is.hail.variant.GenericDataset
import is.hail.variant.Genotype
import au.csiro.variantspark.pedigree.OffspringSpec
import au.csiro.variantspark.pedigree.GenomicPos
import is.hail.variant.Variant
import is.hail.annotations.Annotation
import au.csiro.variantspark.hail.variant.phased.BiCall
import org.apache.spark.sql.Row
import org.apache.spark.rdd.RDD
import is.hail.variant.GTPair
import is.hail.variant._
import is.hail.utils._
import is.hail.expr._
import is.hail.sparkextras.OrderedRDD
import au.csiro.variantspark.pedigree.GenotypeSpec
import au.csiro.variantspark.pedigree.IndexedBiGenotypeSpec
import scala.collection.mutable.ArrayBuffer
import au.csiro.variantspark.pedigree.FamilyTrio
import scala.collection.mutable.HashMap
import au.csiro.variantspark.pedigree.IndividualID



case class OffspringTrio(val trio: FamilyTrio, val offspring: OffspringSpec)  {
  def offspringID = trio.id
  def makeGenotype(position: GenomicPos, population: HashMap[IndividualID, GenotypeSpec[Int]]):GenotypeSpec[Int] = {
    offspring.genotypeAt(position, population(trio.maternalId.get), population(trio.paternalId.get))
  }
}


class FamilyVariantBuilder {
  
  // here assume it's sorted topologically
  val offspring: List[OffspringTrio] = ???
  
  def buildVariant(v: Variant, g: Iterable[Annotation]) {
    
    // wouild be nice if all that could be just done on indexes
    
    val allGenotypes =  HashMap[IndividualID, GenotypeSpec[Int]]()
    
    // first add all funders genotypes
    
    // the construct all offsprings incrementally using updaing the map
    // this probably can be done with fold as well
    offspring.foreach { ot => 
      allGenotypes.put(ot.offspringID, ot.makeGenotype(v, allGenotypes))
    }
    
    // so now the only thing left would be to order the genotypes based on the expeced output labels
    // and the only challenge here is that indeed I need the genotype with  
  }
}


object GenerateFamily {
  
  def apply(offspringSpec:OffspringSpec) = new GenerateOffspring(offspringSpec)
  
  implicit def fromBiCallToGenotypeSpec(bc: BiCall): GenotypeSpec[Int] = IndexedBiGenotypeSpec(bc(0), bc(1))
  implicit def fromGenotypeSpecToBiCall(gs: GenotypeSpec[Int]): BiCall =  BiCall(GTPair.apply(gs(0), gs(1)))
  
  def mapOffspring(rdd: OrderedRDD[Locus, Variant, (Any, Iterable[Annotation])],
      offspringSpec:OffspringSpec):RDD[(Variant, (Any, Iterable[Annotation]))] = {
    
    rdd.mapPartitions(x => x.map { case (v, (a, g)) =>  
      val parentGenotypes = g.toArray
      val motherGenotype = new BiCall(parentGenotypes(0).asInstanceOf[Row].getInt(0))
      val fatherGenotype = new BiCall(parentGenotypes(1).asInstanceOf[Row].getInt(0))
      val offspringGenotype:BiCall =  offspringSpec.genotypeAt(v, motherGenotype, fatherGenotype)
      val offspringAndParent = Annotation.apply(offspringGenotype.p) :: parentGenotypes.toList
      (v, (a,  offspringAndParent)) }, 
    preservesPartitioning = true)  
  }
}

class GenerateFamily(val offspringSpec:OffspringSpec) {
  
  def apply(gds:GenericDataset): GenericDataset =  {
    
    /**
     * I need some notion of the entire family specification
     * Technically this couild be someting like a graf of family trios
     * Maybe a graf then (with keyed labels)
     */
    
    // later on we can probably add all in the form of annotations
    val sampleIds:List[String] = gds.sampleIds.toList.asInstanceOf[List[String]]
    val newIDS = "dsdsdsds" ::  sampleIds
    val transRdd = GenerateFamily.mapOffspring(gds.rdd, offspringSpec)
    gds.copy(rdd = transRdd.asOrderedRDD, sampleIds = newIDS.toIndexedSeq, 
        sampleAnnotations =  Annotation.emptyIndexedSeq(newIDS.length))
  }  
}