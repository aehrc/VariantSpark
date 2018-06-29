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
import au.csiro.variantspark.output.ParquetFeatureSink

class GenerateFeaturesCmd extends ArgsApp with SparkApp with Echoable with Logging with TestArgs {

  @Option(name="-gl", required=false, usage="Number of levels to generate (def=3)", aliases=Array("--gen-n-levels"))
  val nLevels:Int = 3

  @Option(name="-gv", required=false, usage="Number of variables to generate (def=10000)", aliases=Array("--gen-n-variables"))
  val nVariables:Long = 10000L

  @Option(name="-gs", required=false, usage="Number of samples to generate (def=100)", aliases=Array("--gen-n-samples"))
  val nSamples:Int = 100
  

  @Option(name="-of", required=true, usage="Path to output file (def = stdout)", aliases=Array("--output-file") )
  val outputFile:String = null
  
  @Option(name="-ot", required=false, usage="Input file type, one of: vcf, csv (def=vcf)", aliases=Array("--output-type") )
  val outputType:String = null

  @Option(name="-sr", required=false, usage="Random seed to use (def=<random>)", aliases=Array("--seed"))
  val randomSeed: Long = defRng.nextLong

  @Option(name="-sp", required=false, usage="Spark parallelism (def=<default-spark-par>)", aliases=Array("--spark-par"))
  val sparkPar = 0
 
  @Override
  def testArgs = Array("-of", "target/getds.parquet", "-sp", "4")
    
  @Override
  def run():Unit = {
    logInfo("Running with params: " + ToStringBuilder.reflectionToString(this))    
    echo(s"Generating a synthetic dataset, variables: ${nVariables}, samples: ${nSamples}, levels:${nLevels}")
    verbose(s"Random seed is: ${randomSeed}")

    val generator  = new OrdinalFeatureGenerator(nLevels = nLevels, nSamples = nSamples, nVariables = nVariables, seed = randomSeed, sparkPar = sparkPar)
    
    echo(s"Saving output to ${outputFile}")    
    val sink = new ParquetFeatureSink(outputFile)
    sink.save(generator)
   }
}

object GenerateFeaturesCmd  {
  def main(args:Array[String]) {
    AppRunner.mains[GenerateFeaturesCmd](args)
  }
}
