package au.csiro.variantspark.input

import org.apache.spark.rdd.RDD
import collection.JavaConverters._
import au.csiro.variantspark.data.Feature
import au.csiro.variantspark.data.BoundedOrdinalVariable
import au.csiro.variantspark.data.StdFeature

trait VariantToFeatureConverter {
  def convert(v: Variant): Feature
  def convertModeImputed(v: Variant): Feature
  def convertZeroImputed(v: Variant): Feature
}

case class DefVariantToFeatureConverter() extends VariantToFeatureConverter {

  def convert(v: Variant): Feature = {
    StdFeature.from(v.label, BoundedOrdinalVariable(3), v.genotypes)
  }

  def convertModeImputed(v: Variant): Feature = {
    val modeImputedGts = ModeImputationStrategy(noLevels = 3).impute(v.genotypes)
    StdFeature.from(v.label, BoundedOrdinalVariable(3), modeImputedGts)
  }

  def convertZeroImputed(v: Variant): Feature = {
    val zeroImputedGts = ZeroImputationStrategy.impute(v.genotypes)
    StdFeature.from(v.label, BoundedOrdinalVariable(3), zeroImputedGts)
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
  def apply(vcfSource: VCFSource, imputationStrategy: String): VCFFeatureSource = {
    new VCFFeatureSource(vcfSource, DefVariantToFeatureConverter(), imputationStrategy)
  }
}
