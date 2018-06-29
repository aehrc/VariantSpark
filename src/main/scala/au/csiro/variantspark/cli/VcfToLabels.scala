package au.csiro.variantspark.cli

import au.csiro.sparkle.common.args4j.ArgsApp
import au.csiro.sparkle.cmd.CmdApp
import org.kohsuke.args4j.Option
import au.csiro.pbdava.ssparkle.common.arg4j.AppRunner
import au.csiro.pbdava.ssparkle.spark.SparkApp
import collection.JavaConverters._
import au.csiro.variantspark.input.VCFSource
import au.csiro.variantspark.input.VCFFeatureSource
import au.csiro.variantspark.input.HashingLabelSource
import au.csiro.variantspark.algo.WideRandomForest
import org.apache.spark.mllib.linalg.Vectors
import java.io.File
import au.csiro.pbdava.ssparkle.common.utils.LoanUtils
import au.csiro.pbdava.ssparkle.common.utils.CSVUtils


class VcfToLabels extends ArgsApp with SparkApp {

  @Option(name="-if", required=false, usage="This is input files", aliases=Array("--input-file"))
  val inputFile:String = "data/small.vcf"

  @Option(name="-of", required=false, usage="Output file", aliases=Array("--output-file"))
  val outputFile:String = "data/small-labels.csv"

  
  @Option(name="-l", required=false)
  val limit:Int = 10

  
  @Override
  def run():Unit = {
    val vcfSource = VCFSource(sc.textFile(inputFile))
    val header = vcfSource.header
    val version = vcfSource.version  
    println(header)
    println(version)
    val source  = VCFFeatureSource(vcfSource)
    val columns = source.features().take(limit)
    CSVUtils.withFile(new File(outputFile)) { writer =>
      writer.writeRow("" :: columns.map(_.label).toList)
      source.sampleNames.zipWithIndex.foreach { case( row, i) =>
        writer.writeRow(row :: columns.map(_.values(i)).toList)        
      }
    }
    
  }
}

object VcfToLabels  {
  def main(args:Array[String]) {
    AppRunner.mains[VcfToLabels](args)
  }
}
