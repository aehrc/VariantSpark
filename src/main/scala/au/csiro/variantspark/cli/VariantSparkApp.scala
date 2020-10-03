package au.csiro.variantspark.cli

import au.csiro.sparkle.cmd.MultiCmdApp
import au.csiro.pbdava.ssparkle.common.arg4j.AppRunner

class VariantSparkApp extends MultiCmdApp {
  registerClass("analyze-rf", classOf[AnalyzeRFCmd])
  registerClass("build-index", classOf[BuildVarIndexCmd])
  registerClass("convert", classOf[ConvertCmd])
  registerClass("filter", classOf[FilterCmd])
  registerClass("gen-features", classOf[GenerateFeaturesCmd])
  registerClass("gen-labels", classOf[GenerateLabelsCmd])
  registerClass("gen-labels-with-noise", classOf[GenerateLabelsNoiseCmd])
  registerClass("importance", classOf[ImportanceCmd])
  registerClass("importance-ca", classOf[CochranArmanCmd])
  registerClass("null-importance", classOf[NullImportanceCmd])
  registerClass("pdist", classOf[PairWiseDistanceCmd])
  registerClass("predict", classOf[PredictCmd])
  registerClass("test", classOf[TestCmd])
  registerClass("vcf2labels", classOf[VcfToLabels])
}

object VariantSparkApp {
  def main(args: Array[String]) {
    AppRunner.mains[VariantSparkApp](args)
  }
}
