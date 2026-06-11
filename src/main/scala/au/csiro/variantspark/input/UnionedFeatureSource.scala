package au.csiro.variantspark.input

import au.csiro.variantspark.data.Feature
import org.apache.spark.rdd.RDD

case class UnionedFeatureSource(sources: Seq[FeatureSource]) extends FeatureSource {
  require(sources.nonEmpty, "UnionedFeatureSource requires at least one source")
  require(sources.tail.forall(_.sampleNames == sources.head.sampleNames),
    s"All sources must have identical sample names in identical order; " +
      s"first mismatch at index ${sources.indexWhere(_.sampleNames != sources.head.sampleNames)}")

  def sampleNames: List[String] = sources.head.sampleNames

  def features: RDD[Feature] =
    sources.map(_.features).reduce(_.union(_))
}
