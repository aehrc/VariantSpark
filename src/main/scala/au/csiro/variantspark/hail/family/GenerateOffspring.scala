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


object GenerateOffspring {
  
  def apply(offspringSpec:OffspringSpec) = new GenerateOffspring(offspringSpec)
  
  implicit def fromVariantToGenomicPos(v: Variant): GenomicPos = GenomicPos(v.contig, v.start)
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

class GenerateOffspring(val offspringSpec:OffspringSpec) {
  
  def apply(gds:GenericDataset): GenericDataset =  {
    
    // later on we can probably add all in the form of annotations
    val sampleIds:List[String] = gds.sampleIds.toList.asInstanceOf[List[String]]
    val newIDS = "dsdsdsds" ::  sampleIds
    val transRdd = GenerateOffspring.mapOffspring(gds.rdd, offspringSpec)
    gds.copy(rdd = transRdd.asOrderedRDD, sampleIds = newIDS.toIndexedSeq, 
        sampleAnnotations =  Annotation.emptyIndexedSeq(newIDS.length))
  }  
}