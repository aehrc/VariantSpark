package au.csiro.variantspark.hail.adapter


import au.csiro.variantspark.input.FeatureSource
import org.apache.spark.rdd.RDD
import au.csiro.variantspark.data._
import is.hail.variant.Genotype
import au.csiro.variantspark.input._
import au.csiro.variantspark.data.BoundedOrdinalVariable
import au.csiro.variantspark.data.Feature

/** Implements the variant-spark FeatureSource on a Hail VariantDataset. 
  * Sample names come from VariantDataset samples the genotypes are encoded as 0, 1 or 2 for 
  * base homo, hetero and alt homo alleles respectively. Uncalled genotypes are encoded as 0.
  * 
  * @param vds a hail VariantDataset
  */
class HailFeatureSource 
//(val vds: VariantDataset) extends FeatureSource {
//  
//  def sampleNames:List[String] = vds.sampleIds.map(_.toString()).toList
//
//  def features:RDD[Feature] = vds.rdd.map { 
//    case (variant, (_, genotypes)) => HailFeatureSource.hailLineToFeature(variant, genotypes) }
//}
//
//object HailFeatureSource {
//  
//  val DEFAULT_VARIABLE_TYPE =  BoundedOrdinalVariable(3)
//  
//  def apply(vds: VariantDataset) = new HailFeatureSource(vds)
//  
//  def variantToFeatureName(variant:Variant) = List(variant.contig, variant.start,
//        variant.ref, variant.alt).mkString(":")
//  
//  private def hailLineToFeature[V](variant:Variant, genotypes:Iterable[Genotype]):Feature = {    
//    StdFeature.from(variantToFeatureName(variant), DEFAULT_VARIABLE_TYPE, genotypes.map(genotypeToHamming).toArray)
//  }
//  
//  private def genotypeToHamming(gt:Genotype):Byte = if (!gt.isCalled || gt.isHomRef) 0 else if (gt.isHomVar) 2 else 1
//}