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
import au.csiro.variantspark.pedigree.FamilySpec
import au.csiro.variantspark.pedigree.OffspringTrio


/**
 * @param sampleIds: list of the ids all genotypes
 * @param founderIds: list of ids for founder samples 
 * @param offspring: topologically sorted list of offsprings 
 */
class FamilyVariantBuilder(val sampleIds:IndexedSeq[Annotation], val founderIds:List[Annotation],
      val offspring: List[OffspringTrio] ) extends Serializable {
  
  def buildVariant(v: Variant, g: Iterable[Annotation]):Iterable[Annotation] =  {
    
    // TODO: OPTIMIZE: Make a version that works on indexes
    val allGenotypes =  HashMap[IndividualID, GenotypeSpec[Int]]()
    // index all genotypes with their idss
    val genotypeBySampleId = sampleIds.zip(g).toMap.mapValues(a => new BiCall(a.asInstanceOf[Row].getInt(0)))
    
    // add founder genotypes to the pool
    founderIds.foreach { fid => 
      allGenotypes.put(fid.asInstanceOf[IndividualID], genotypeBySampleId(fid))
    }
    // the construct all offsprings incrementally using updaing the map
    // this probably can be done with fold as well
    offspring.foreach { ot => 
      allGenotypes.put(ot.offspringID, ot.makeGenotype(v, allGenotypes))
    }
    
    // TODO: add also offspring here
    founderIds.map(fid => Annotation.apply(allGenotypes(fid.asInstanceOf[IndividualID]).p))
  }
}


object GenerateFamily {
  def apply(familySpec:FamilySpec) = new GenerateFamily(familySpec)  
}

class GenerateFamily(val familySpec: FamilySpec) {
  
  def apply(gds:GenericDataset): GenericDataset =  {
    // Check if all the founders  are avaliable in the family 
    // and later that they have correct sex (for their roles?) 
    
    val sampleIds:IndexedSeq[Annotation] = gds.sampleIds
    val founders = familySpec.founders 
    //assert(gds.sampleIds.toSet.
    val familyIDs:List[String] = familySpec.individuals
    
    val variantBuilder = new FamilyVariantBuilder(
        sampleIds = sampleIds, 
        founderIds = familySpec.founders.asInstanceOf[List[Annotation]],
        offspring = familySpec.offspring
    )
    val br_variantBuilder = gds.rdd.sparkContext.broadcast(variantBuilder)
    val familyRdd = gds.rdd.mapPartitions({it => 
        val localVariantBuilder = br_variantBuilder.value
        it.map { case (v, (a, g)) => 
          (v, (a, localVariantBuilder.buildVariant(v, g)))
        }
      },preservesPartitioning = true)
            
    gds.copy(rdd = familyRdd.asOrderedRDD, sampleIds = familyIDs.toIndexedSeq, 
        sampleAnnotations =  Annotation.emptyIndexedSeq(familyIDs.length))
  }  
}