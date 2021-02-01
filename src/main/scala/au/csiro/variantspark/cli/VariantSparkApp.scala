package au.csiro.variantspark.cli

import au.csiro.pbdava.ssparkle.common.arg4j.AppRunner
import au.csiro.sparkle.cmd.MultiCmdApp

class VariantSparkApp extends MultiCmdApp {
  registerClass("test", classOf[TestCmd])
  registerClass("filter", classOf[FilterCmd])
  registerClass("importance", classOf[ImportanceCmd])
  registerClass("null-importance", classOf[NullImportanceCmd])
  registerClass("importance-ca", classOf[CochranArmanCmd])
  registerClass("gen-features", classOf[GenerateFeaturesCmd])
  registerClass("gen-labels", classOf[GenerateLabelsCmd])
  registerClass("gen-labels-with-noise", classOf[GenerateLabelsNoiseCmd])
  registerClass("convert", classOf[ConvertCmd])
  registerClass("analyze-rf", classOf[AnalyzeRFCmd])
  registerClass("build-index", classOf[BuildVarIndexCmd])
  registerClass("pdist", classOf[PairWiseDistanceCmd])
  registerClass("version", classOf[VersionCmd])
}

object VariantSparkApp {
  def main(args: Array[String]) {
    AppRunner.mains[VariantSparkApp](args)
  }
}
