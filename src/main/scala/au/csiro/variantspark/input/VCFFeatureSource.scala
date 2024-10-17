package au.csiro.variantspark.input

import org.apache.spark.rdd.RDD
import htsjdk.variant.variantcontext.Genotype
import htsjdk.variant.variantcontext.VariantContext
import collection.JavaConverters._
import au.csiro.variantspark.data.Feature
import au.csiro.variantspark.data.BoundedOrdinalVariable
import au.csiro.variantspark.data.StdFeature

trait VariantToFeatureConverter {
  def convert(vc: VariantContext): Feature
  def convertModeImputed(vc: VariantContext): Feature
  def convertZeroImputed(vc: VariantContext): Feature
}

case class DefVariantToFeatureConverter(biallelic: Boolean = false, separator: String = "_")
    extends VariantToFeatureConverter {

  def convert(vc: VariantContext): Feature = {
    val gts = vc.getGenotypes.iterator().asScala.map(convertGenotype).toArray
    StdFeature.from(convertLabel(vc), BoundedOrdinalVariable(3), gts)
  }

  def convertModeImputed(vc: VariantContext): Feature = {
    val gts = vc.getGenotypes.iterator().asScala.map(convertGenotype).toArray
    val modeImputedGts = ModeImputationStrategy(noLevels = 3).impute(gts)
    StdFeature.from(convertLabel(vc), BoundedOrdinalVariable(3), modeImputedGts)
  }

  def convertZeroImputed(vc: VariantContext): Feature = {
    val gts = vc.getGenotypes.iterator().asScala.map(convertGenotype).toArray
    val zeroImputedGts = ZeroImputationStrategy.impute(gts)
    StdFeature.from(convertLabel(vc), BoundedOrdinalVariable(3), zeroImputedGts)
  }

  def convertLabel(vc: VariantContext): String = {

    if (biallelic && !vc.isBiallelic) {
      throw new IllegalArgumentException(
          s"Variant ${vc.toStringWithoutGenotypes} is not biallelic!")
    }
    val labelBuilder = new StringBuilder()
    labelBuilder
      .append(vc.getContig)
      .append(separator)
      .append(vc.getStart)
      .append(separator)
      .append(vc.getReference.getBaseString)
    if (biallelic) {
      labelBuilder.append(separator).append(vc.getAlternateAllele(0).getBaseString)
    } else {
      labelBuilder
        .append(separator)
        .append(vc.getAlternateAlleles.asScala.map(_.getBaseString()).mkString("|"))
    }
    labelBuilder.toString()
  }

  def convertGenotype(gt: Genotype): Byte = {
    if (!gt.isCalled) Missing.BYTE_NA_VALUE
    else if (gt.isHomRef) 0
    else if (gt.isHomVar || gt.isHetNonRef) 2
    else 1
  }
}

class VCFFeatureSource(vcfSource: VCFSource, converter: VariantToFeatureConverter,
    imputationStrategy: String)
    extends FeatureSource {
  override lazy val sampleNames: List[String] =
    vcfSource.header.getGenotypeSamples.asScala.toList
  override def features: RDD[Feature] = {
    val converterRef = converter
    imputationStrategy match {
      case "none" => vcfSource.genotypes().map(converterRef.convert)
      case "mode" => vcfSource.genotypes().map(converterRef.convertModeImputed)
      case "zeros" => vcfSource.genotypes().map(converterRef.convertZeroImputed)
      case _ =>
        throw new IllegalArgumentException(s"Unknown imputation strategy: $imputationStrategy")
    }
  }
}

object VCFFeatureSource {
  def apply(vcfSource: VCFSource, imputationStrategy: String, biallelic: Boolean = false,
      separator: String = "_"): VCFFeatureSource = {
    new VCFFeatureSource(vcfSource, DefVariantToFeatureConverter(biallelic, separator),
      imputationStrategy)
  }
}
