package au.csiro.variantspark.cli

import au.csiro.sparkle.cmd.MultiCmdApp
import au.csiro.pbdava.ssparkle.common.arg4j.AppRunner


class VariantSparkApp extends MultiCmdApp {
  registerClass("test", classOf[TestCmd])  
  registerClass("filter", classOf[FilterCmd])  
  registerClass("importance", classOf[ImportanceCmd])  
  registerClass("gen-features", classOf[GenerateFeaturesCmd])  
}

object VariantSparkApp {
  def main(args:Array[String]) {
    AppRunner.mains[VariantSparkApp](args)
  }
}