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
import au.csiro.variantspark.input.CsvLabelSource
import au.csiro.variantspark.cmd.Echoable
import au.csiro.pbdava.ssparkle.common.utils.Logging
import org.apache.commons.lang3.builder.ToStringBuilder
import au.csiro.variantspark.cmd.EchoUtils._
import au.csiro.pbdava.ssparkle.common.utils.LoanUtils
import au.csiro.pbdava.ssparkle.common.arg4j.TestArgs
import org.apache.hadoop.fs.FileSystem
import au.csiro.variantspark.algo.WideDecisionTree
import org.apache.commons.math3.stat.correlation.PearsonsCorrelation
import au.csiro.pbdava.ssparkle.spark.SparkUtils
import au.csiro.pbdava.ssparkle.common.utils.ReusablePrintStream
import au.csiro.variantspark.algo.RandomForestCallback
import au.csiro.variantspark.utils.VectorRDDFunction._
import au.csiro.variantspark.input.CsvFeatureSource
import au.csiro.variantspark.algo.RandomForestParams
import au.csiro.variantspark.data.BoundedOrdinal
import au.csiro.pbdava.ssparkle.common.utils.Timer
import au.csiro.variantspark.utils.defRng
import au.csiro.variantspark.input.generate.OrdinalFeatureGenerator
import au.csiro.variantspark.output.CSVFeatureSink
import au.csiro.variantspark.input.ParquetFeatureSource
import au.csiro.variantspark.input.generate.EffectLabelGenerator
import java.io.File
import java.util.ArrayList
import au.csiro.variantspark.cli.args.FeatureSourceArgs
import au.csiro.variantspark.output.CSVFeatureSink2

class ConvertCmd extends ArgsApp with FeatureSourceArgs with Echoable with Logging with TestArgs {

  @Option(name="-of", required=true, usage="Path to output file", aliases=Array("--output-file") )
  val outputFile:String = null
  
  @Option(name="-ot", required=false, usage="Input file type, one of: vcf, csv (def=vcf)", aliases=Array("--output-type") )
  val outputType:String = null  
   
  @Override
  def testArgs = Array("-if", "data/chr22_1000.vcf", "-it", "vcf", "-sp", "4", "-of", "target/getds.csv")
    
  @Override
  def run():Unit = {
    logInfo("Running with params: " + ToStringBuilder.reflectionToString(this))    
  
    echo(s"Converting from: ${inputType} to: ${outputType}")
    
    echo(s"Loading parquet file: ${inputFile}")
    echo(s"Loaded rows: ${dumpList(featureSource.sampleNames)}")  
    
    val sink = CSVFeatureSink2(outputFile)
    sink.save(featureSource)
  }    
}

object ConvertCmd  {
  def main(args:Array[String]) {
    AppRunner.mains[ConvertCmd](args)
  }
}
