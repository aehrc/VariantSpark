package au.csiro.variantspark.cli

import au.csiro.sparkle.common.args4j.ArgsApp
import au.csiro.sparkle.cmd.CmdApp
import org.kohsuke.args4j.Option
import au.csiro.pbdava.ssparkle.common.arg4j.AppRunner
import au.csiro.pbdava.ssparkle.spark.SparkApp
import collection.JavaConverters._
import au.csiro.variantspark.input.VCFSource
import au.csiro.variantspark.input.VCFFeatureSource


class VcfToFeatures extends ArgsApp with SparkApp {

  @Option(name="-if", required=false, usage="This is input files", aliases=Array("--input-files"))
  val inputFile:String = "data/small.vcf"

  @Option(name="-l", required=false)
  val limit:Int = 0

  
  @Override
  def run():Unit = {
   
    
    println("Hello Word: " + inputFile + ", " + limit)
    val vcfSource = VCFSource(sc.textFile(inputFile))
    val header = vcfSource.header
    val version = vcfSource.version  
    println(header)
    println(version)
    VCFFeatureSource(vcfSource).features().map(_.toList).foreach(println)
    println("Done")
    
  }
}

object VcfToFeatures  {
  def main(args:Array[String]) {
    AppRunner.mains[VcfToFeatures](args)
  }
}
