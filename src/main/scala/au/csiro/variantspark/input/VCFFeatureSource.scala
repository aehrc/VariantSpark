package au.csiro.variantspark.input

import org.apache.spark.rdd.RDD
import htsjdk.variant.variantcontext.Genotype
import htsjdk.variant.variantcontext.VariantContext
import collection.JavaConverters._

class VCFFeatureSource(vcfSource:VCFSource) extends FeatureSource {
  
  override lazy val sampleNames:List[String] = vcfSource.header.getGenotypeSamples().asScala.toList
  override def features():RDD[Feature] = vcfSource.genotypes().map(VCFFeatureSource.variantToFeature(VCFFeatureSource.hammingConversion)) 
}

object VCFFeatureSource {
  
  def apply(vcfSource:VCFSource) = new VCFFeatureSource(vcfSource)
  
  private def variantToFeature(f:Genotype=>Byte)(vc:VariantContext) =  Feature(vc.getContig() + "_" + vc.getEnd(), vc.getGenotypes.iterator().asScala.map(f).toArray)
  private def hammingConversion(gt:Genotype):Byte = if (gt.isHomRef()) 0 else if (gt.isHomVar()) 2 else 1
}