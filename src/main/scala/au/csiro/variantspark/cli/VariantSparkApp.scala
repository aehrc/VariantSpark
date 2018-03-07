package au.csiro.variantspark.cli

import au.csiro.sparkle.cmd.MultiCmdApp
import au.csiro.pbdava.ssparkle.common.arg4j.AppRunner


class VariantSparkApp extends MultiCmdApp {
  registerClass("test", classOf[TestCmd])  
  registerClass("filter", classOf[FilterCmd])  
  registerClass("importance", classOf[ImportanceCmd])  
  registerClass("importance-ca", classOf[CochranArmanCmd])  
  registerClass("gen-features", classOf[GenerateFeaturesCmd])  
  registerClass("gen-labels", classOf[GenerateLabelsCmd])  
  registerClass("gen-labels-with-noise", classOf[GenerateLabelsNoiseCmd])  
  registerClass("convert", classOf[ConvertCmd])  
  registerClass("analyze-rf", classOf[AnalyzeRFCmd])  
  registerClass("build-index", classOf[BuildVarIndexCmd])  
  registerClass("pdist", classOf[PairWiseDistanceCmd])  
  registerClass("gen-family", classOf[GenerateFamilyCmd])  
  registerClass("gen-pop", classOf[GeneratePopulationCmd])  
}

object VariantSparkApp {
  def main(args:Array[String]) {
    AppRunner.mains[VariantSparkApp](args)
  }
}