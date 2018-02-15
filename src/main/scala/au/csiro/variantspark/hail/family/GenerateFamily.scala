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
import scala.collection.mutable.ArrayBuffer
import au.csiro.variantspark.pedigree.FamilyTrio
import scala.collection.mutable.HashMap
import au.csiro.variantspark.pedigree.IndividualID
import au.csiro.variantspark.pedigree.FamilySpec
import au.csiro.variantspark.pedigree.GenotypePool
import au.csiro.variantspark.pedigree.MutableVariant
import au.csiro.variantspark.pedigree.DefMutableVariant


/**
 * @param sampleIds: list of the ids all genotypes
 * @param founderIds: list of ids for founder samples 
 * @param offspring: topologically sorted list of offsprings 
 */
class FamilyVariantBuilder(val sampleIds:IndexedSeq[Annotation], val familySpec: FamilySpec) extends Serializable {
  
  def buildVariant(v: Variant, g: Iterable[Annotation]):Option[Iterable[Annotation]] =  {
    
    // construct the initial pool from genotype samples
    val initialPool:GenotypePool = sampleIds.map(_.asInstanceOf[String])
      .zip(g).toMap.mapValues(a => new BiCall(a.asInstanceOf[Row].getInt(0)))

      
    val mv = new DefMutableVariant(GenomicPos(v.contig, v.start), v.ref, v.altAlleles.map(_.alt))
    // filter out positions with no variants in initial pool
    // TODO: Optmization: Do this before computing the genotypes
    // TODO: Func: Update the variant if needed
    val outputPool = familySpec.produceGenotypePool(mv, initialPool)
    val hasNoVariants = outputPool.values.forall(gs => gs(0) == 0 && gs(1) == 0) 
    
    if (hasNoVariants) {
      None
    } else {
      // create the output pool for this family
      // convert the pool back to list in the oder of family members
      Some(familySpec.memberIds.map(mid => Annotation.apply(outputPool(mid.asInstanceOf[IndividualID]).p)))
    }
    
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
    val familyIDs:List[String] = familySpec.memberIds
    
    val variantBuilder = new FamilyVariantBuilder(sampleIds, familySpec)
    val br_variantBuilder = gds.rdd.sparkContext.broadcast(variantBuilder)
    val familyRdd = gds.rdd.mapPartitions({it => 
        val localVariantBuilder = br_variantBuilder.value
        it.flatMap { case (v, (a, g)) => 
          localVariantBuilder.buildVariant(v, g).map(i => (v, (a, i))) 
        }
      },preservesPartitioning = true)
            
    //TODO: Optimization: Filter out all base variant call (there will be may since we 
    // we narow down the initial population
      
    gds.copy(rdd = familyRdd.asOrderedRDD, sampleIds = familyIDs.toIndexedSeq, 
        sampleAnnotations =  Annotation.emptyIndexedSeq(familyIDs.length))
  }  
}