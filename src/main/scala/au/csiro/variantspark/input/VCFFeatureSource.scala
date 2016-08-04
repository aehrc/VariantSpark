package au.csiro.variantspark.input

import org.apache.spark.rdd.RDD
import htsjdk.variant.variantcontext.Genotype
import htsjdk.variant.variantcontext.VariantContext
import collection.JavaConverters._

class VCFFeatureSource(vcfSource:VCFSource) {
  def features():RDD[Array[Int]] = vcfSource.genotypes().map(VCFFeatureSource.variantToFeature(VCFFeatureSource.hammingConversion)) 
}

object VCFFeatureSource {
  
  def apply(vcfSource:VCFSource) = new VCFFeatureSource(vcfSource)
  
  private def variantToFeature(f:Genotype=>Int)(vc:VariantContext) =  vc.getGenotypes.iterator().asScala.map(f).toArray 
  private def hammingConversion(gt:Genotype) = if (gt.isHomRef()) 0 else if (gt.isHomVar()) 2 else 1
}