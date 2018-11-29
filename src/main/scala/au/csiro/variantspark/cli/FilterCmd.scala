package au.csiro.variantspark.cli

import au.csiro.sparkle.common.args4j.ArgsApp
import au.csiro.sparkle.cmd.CmdApp
import org.kohsuke.args4j.Option
import au.csiro.pbdava.ssparkle.common.arg4j.AppRunner
import collection.JavaConverters._
import au.csiro.pbdava.ssparkle.common.arg4j.TestArgs
import au.csiro.pbdava.ssparkle.spark.SparkApp
import org.apache.hadoop.fs.FileSystem
import au.csiro.pbdava.ssparkle.common.utils.LoanUtils._
import org.apache.hadoop.fs.Path
import au.csiro.variantspark.input.VCFFeatureSource
import au.csiro.variantspark.input.VCFSource
import au.csiro.pbdava.ssparkle.common.utils.Timed


class FilterCmd extends ArgsApp with TestArgs with SparkApp {

  @Option(name="-if", required=true, usage="This is input files", aliases=Array("--input-files"))
  val inputFile:String = null

  @Option(name="-l", required=false)
  val limit:Int = 0

  @Override
  def testArgs = Array("-if", "data/small.vcf")  

  @Override
  def run():Unit = {
    implicit val fs = FileSystem.get(sc.hadoopConfiguration)  
    logDebug(s"Running with filesystem: ${fs}, home: ${fs.getHomeDirectory}")
    
    val vcfSource = VCFSource(sc.textFile(inputFile))
    val source  = VCFFeatureSource(vcfSource)
    val features = source.features.zipWithIndex().cache()
    val featureCount = features.count()
    println(s"No features: ${featureCount}")

   }     
}

object FilterCmd  {
  def main(args:Array[String]) {
    AppRunner.mains[FilterCmd](args)
  }
}
