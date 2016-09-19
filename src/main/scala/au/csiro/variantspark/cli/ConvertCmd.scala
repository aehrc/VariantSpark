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
import org.apache.spark.Logging
import org.apache.commons.lang3.builder.ToStringBuilder
import au.csiro.variantspark.cmd.EchoUtils._
import au.csiro.pbdava.ssparkle.common.utils.LoanUtils
import com.github.tototoshi.csv.CSVWriter
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
import au.csiro.variantspark.input.generate.EfectLabelGenerator
import java.io.File
import java.util.ArrayList

class ConvertCmd extends ArgsApp with SparkApp with Echoable with Logging with TestArgs {

  // input options
  @Option(name="-if", required=true, usage="Path to input file or directory", aliases=Array("--input-file"))
  val inputFile:String = null

  @Option(name="-it", required=false, usage="Input file type, one of: vcf, csv (def=parquet)", aliases=Array("--input-type"))
  val inputType:String = "parquet"

 // output options
  @Option(name="-of", required=true, usage="Path to output file", aliases=Array("--output-file") )
  val outputFile:String = null
  
  @Option(name="-ot", required=false, usage="Input file type, one of: vcf, csv (def=vcf)", aliases=Array("--output-type") )
  val outputType:String = null  
  
  // spark related
  @Option(name="-sp", required=false, usage="Spark parallelism (def=<default-spark-par>)", aliases=Array("--spark-par"))
  val sparkPar = 0
 
  @Override
  def testArgs = Array("-if", "target/getds.parquet", "-sp", "4", "-of", "target/getds.csv")
    
  @Override
  def run():Unit = {
    logInfo("Running with params: " + ToStringBuilder.reflectionToString(this))    
  
    echo(s"Converting from: ${inputType} to: ${outputType}")
    
    echo(s"Loading parquet file: ${inputFile}")
    val featureSource = new ParquetFeatureSource(inputFile)
    echo(s"Loaded rows: ${dumpList(featureSource.sampleNames)}")  
    
    val sink = CSVFeatureSink(outputFile)
    sink.save(featureSource)
  }    
}

object ConvertCmd  {
  def main(args:Array[String]) {
    AppRunner.mains[ConvertCmd](args)
  }
}
