package au.csiro.variantspark.hail.family

import is.hail.variant.GenericDataset
import is.hail.variant.Genotype
import au.csiro.variantspark.pedigree.OffspringSpec
import au.csiro.variantspark.pedigree.GenomicPos
import is.hail.variant.Variant
import is.hail.annotations.Annotation
import au.csiro.variantspark.hail.variant.phased.BiCall
import org.apache.spark.sql.Row
import is.hail.variant.GTPair
import is.hail.variant._
import is.hail.utils._
import is.hail.expr._


object GenerateOffspring {
  
  def createOffspringGeneric(v:Variant, genotypes:Iterable[Annotation]): Iterable[Annotation] = {
   
    // genotypes is iterable od row
    // create a random mixtue of parents
   
    val parentGenotypes = genotypes.toArray
    val motherGenotype = new BiCall(parentGenotypes(0).asInstanceOf[Row].getInt(0))
    val fatherGenotype = new BiCall(parentGenotypes(1).asInstanceOf[Row].getInt(0))
    
    
    // here come the randomisation
    val offspringGenotype = GTPair(motherGenotype(0), fatherGenotype(1))
    
    val offspringAndParent = Annotation(offspringGenotype.p) :: parentGenotypes.toList
    offspringAndParent
  } 

}

class GenerateOffspring(val offspringSpec:OffspringSpec) {
  

  def apply(gds:GenericDataset): GenericDataset =  {
    
    // later on we can probably add all in the form of annotations
    val sampleIds:List[String] = gds.sampleIds.toList.asInstanceOf[List[String]]
    val newIDS = "dsdsdsds" ::  sampleIds
    
    val transRdd = gds.rdd.mapPartitions(x => x.map { case (v, (a, g)) =>
        (v, (a, GenerateOffspring.createOffspringGeneric(v, g))) }, preservesPartitioning = true)  
        
    val offsrings = gds.copy(rdd = transRdd.asOrderedRDD, sampleIds = newIDS.toIndexedSeq, 
        sampleAnnotations =  Annotation.emptyIndexedSeq(newIDS.length))

    return null
  }
  
}