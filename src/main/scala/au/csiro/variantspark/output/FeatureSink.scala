package au.csiro.variantspark.output

import au.csiro.variantspark.input.FeatureSource

trait FeatureSink {
  def save(fs: FeatureSource)
}
