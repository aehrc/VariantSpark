package au.csiro.variantspark.input

import au.csiro.variantspark.data.Feature
import org.apache.spark.rdd.RDD

case class UnionedFeatureSource(featureSource: FeatureSource, covariateSource: FeatureSource)
    extends FeatureSource {
  def sampleNames: List[String] = featureSource.sampleNames

  def features: RDD[Feature] = {
    val features = featureSource.features
    val covariates = covariateSource.features
    features.union(covariates)
  }
}
