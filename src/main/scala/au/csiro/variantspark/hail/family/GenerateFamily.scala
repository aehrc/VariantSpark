package au.csiro.variantspark.hail.family

import is.hail.variant.GenericDataset
import is.hail.variant.Genotype
import au.csiro.variantspark.genomics.GenomicCoord
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
import au.csiro.variantspark.genomics.GenotypeSpec
import scala.collection.mutable.ArrayBuffer
import au.csiro.variantspark.genomics.family.FamilyTrio
import scala.collection.mutable.HashMap
import au.csiro.variantspark.genomics.IndividualID
import au.csiro.variantspark.genomics.family.FamilySpec
import au.csiro.variantspark.genomics.GenotypePool
import au.csiro.variantspark.genomics.ContigID
import au.csiro.variantspark.genomics.BasesVariant
import au.csiro.variantspark.genomics.IndexedVariant
import au.csiro.variantspark.genomics.reprod.MutableVariant

class HailMutableVariantAdapter(v: Variant) extends MutableVariant {
  def contig: ContigID = v.contig
  def pos: Long  = v.start
  def ref: BasesVariant = v.ref
  private var mutated = false
  private lazy val altBuffer = v.altAlleles.map(_.alt).toBuffer
  
  def getOrElseUpdate(base: BasesVariant): IndexedVariant = {
    if (ref == base) 0 else {
      val index = altBuffer.indexOf(base)
      if (index >= 0) index + 1 else {
        mutated = true
        (altBuffer+=base).size
      }
    }
  }
 
  def toVariant: Variant = {
    if (mutated) Variant(v.contig, v.start, v.ref, altBuffer.toArray) else v
  }
  
}

/**
 * @param sampleIds: list of the ids all genotypes
 * @param founderIds: list of ids for founder samples 
 * @param offspring: topologically sorted list of offsprings 
 */
class FamilyVariantBuilder(val sampleIds:IndexedSeq[Annotation], val familySpec: FamilySpec) extends Serializable {
  
  def buildVariant(v: Variant, g: Iterable[Annotation]):Option[(Variant, Iterable[Annotation])] =  {
    
    // construct the initial pool from genotype samples
    val initialPool:GenotypePool = sampleIds.map(_.asInstanceOf[String])
      .zip(g).toMap.mapValues(a => new BiCall(a.asInstanceOf[Row].getInt(0)))

      
    val mv = new HailMutableVariantAdapter(v)
    // filter out positions with no variants in initial pool
    // TODO: Optmization: Do this before computing the genotypes
    val outputPool = familySpec.produceGenotypePool(mv, initialPool)
    val hasNoVariants = outputPool.values.forall(gs => gs(0) == 0 && gs(1) == 0) 
    
    if (hasNoVariants) {
      None
    } else {
      // create the output pool for this family
      // convert the pool back to list in the oder of family members
      Some((mv.toVariant, 
          familySpec.memberIds.map(mid => Annotation.apply(outputPool(mid.asInstanceOf[IndividualID]).p))))
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
          localVariantBuilder.buildVariant(v, g).map(i => (i._1, (a, i._2))) 
        }
      },preservesPartitioning = true)
            
    //TODO: Optimization: Filter out all base variant call (there will be may since we 
    // we narow down the initial population
      
    gds.copy(rdd = familyRdd.asOrderedRDD, sampleIds = familyIDs.toIndexedSeq, 
        sampleAnnotations =  Annotation.emptyIndexedSeq(familyIDs.length))
  }  
}