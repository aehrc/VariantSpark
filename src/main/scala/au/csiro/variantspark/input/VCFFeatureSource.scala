package au.csiro.variantspark.input

import org.apache.spark.rdd.RDD
import htsjdk.variant.variantcontext.Genotype
import htsjdk.variant.variantcontext.VariantContext
import collection.JavaConverters._


trait VariantToFeatureConverter {
  def convert(v:VariantContext):Feature
}

case class DefVariantToFeatureConverter(biallelic:Boolean = false, separator:String = "_") extends VariantToFeatureConverter {
  def convert(vc:VariantContext):Feature = {
    Feature(convertLabel(vc), vc.getGenotypes.iterator().asScala.map(convertGenotype).toArray)
  }
  
  def convertLabel(vc:VariantContext):String = {
   
    if (biallelic && !vc.isBiallelic()) {
      throw new IllegalArgumentException(s"Variant ${vc.toStringWithoutGenotypes()} is not biallelic!")
    }
    val labelBuilder = new StringBuilder()
    labelBuilder.append(vc.getContig()).append(separator).append(vc.getStart())
      .append(separator).append(vc.getReference().getBaseString())
    if (biallelic) {
      labelBuilder.append(separator).append(vc.getAlternateAllele(0).getBaseString())
    }
    labelBuilder.toString()
  }
  
  def convertGenotype(gt:Genotype):Byte = {
    if (!gt.isCalled() || gt.isHomRef()) 0 else if (gt.isHomVar() || gt.isHetNonRef()) 2 else 1
  }
}

class VCFFeatureSource(vcfSource:VCFSource, converter: VariantToFeatureConverter) extends FeatureSource {
  override lazy val sampleNames:List[String] = vcfSource.header.getGenotypeSamples().asScala.toList
  override def features():RDD[Feature] = {
    val converterRef = converter
    vcfSource.genotypes().map(converterRef.convert) 
  }
}

object VCFFeatureSource {
  def apply(vcfSource:VCFSource, biallelic:Boolean = false, separator:String = "_") = {
    new VCFFeatureSource(vcfSource, DefVariantToFeatureConverter(biallelic, separator))
  }
}