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
import au.csiro.variantspark.algo.WideRandomForestCallback
import au.csiro.variantspark.utils.VectorRDDFunction._
import au.csiro.variantspark.input.CsvFeatureSource
import au.csiro.variantspark.algo.RandomForestParams
import au.csiro.variantspark.data.BoundedOrdinal
import au.csiro.pbdava.ssparkle.common.utils.Timer
import au.csiro.variantspark.utils.defRng
import au.csiro.variantspark.input.generate.OrdinalFeatureGenerator
import au.csiro.variantspark.output.ParquetFeatureSink
import au.csiro.variantspark.input.ParquetFeatureSource
import au.csiro.variantspark.input.generate.EfectLabelGenerator
import java.io.File
import java.util.ArrayList

class GenerateLabelsCmd extends ArgsApp with SparkApp with Echoable with Logging with TestArgs {

  // input options
  @Option(name="-if", required=true, usage="Path to input file or directory", aliases=Array("--input-file"))
  val inputFile:String = null

  @Option(name="-it", required=false, usage="Input file type, one of: vcf, csv (def=parquet)", aliases=Array("--input-type"))
  val inputType:String = "parquet"
  
  @Option(name="-ivo", required=false, usage="Variable type ordinal with this number of levels (def = 3)" 
      , aliases=Array("--input-var-ordinal"))
  val varOrdinalLevels:Int = 3;

  // output options

  @Option(name="-ff", required=true, usage="Path to feature file", aliases=Array("--feature-file"))
  val featuresFile:String = null

  @Option(name="-fc", required=true, usage="Name of the feature column", aliases=Array("--feature-column"))
  val featureColumn:String = null
  
  // generator options
  @Option(name="-gs", required=false, usage="Generator effect noise stddev (def=0.0)", aliases=Array("--gen-noise-sigma"))
  val noiseSigma:Double = 0.0

  @Option(name="-gm", required=false, usage="Generator effect noise man (def=0.0)", aliases=Array("--gen-noise-mean"))
  val noiseMean:Double = 0.0

  @Option(name="-gvf", required=false, usage="Generator fraction of noise variables (def=0.0)", aliases=Array("--gen-noise-fraction"))
  val noiseVarFraction:Double = 0.0
  
  @Option(name="-gz", required=false, usage="Generator zero level (def = <factor-levels>/2)", aliases=Array("--gen-zero-level"))
  val zeroLevel:Int = -1
 
  @Option(name="-ge", required=false, usage="Generator effects <var-name>:<effect-size> (can be used may times)", aliases=Array("--gen-effect"))
  val effectsDef:ArrayList[String] = new ArrayList()
  
   // common options
  
  @Option(name="-sr", required=false, usage="Random seed to use (def=<random>)", aliases=Array("--seed"))
  val randomSeed: Long = defRng.nextLong
  
  // spark related
  @Option(name="-sp", required=false, usage="Spark parallelism (def=<default-spark-par>)", aliases=Array("--spark-par"))
  val sparkPar = 0
 
  @Override
  def testArgs = Array("-if", "target/getds.parquet", "-sp", "4", "-ff", "target/features.csv", "-fc", "resp", "-ge", "0:1.0", "-ge", "1:2.0",
      "-gm",  "0.01",  "-gvf",  "0.001",  "-gs", "-0.01")
    
  @Override
  def run():Unit = {
    logInfo("Running with params: " + ToStringBuilder.reflectionToString(this))    
  
    val actualZeroLevel = if (zeroLevel > 0) zeroLevel else varOrdinalLevels/2 
    echo(s"Generating a dichotomous response, zeroLevel: ${actualZeroLevel}, noiseSigma: ${noiseSigma}")
    
    val effects = effectsDef.asScala.map(_.split(":")).map {case Array(v, e) => (v, e.toDouble)}.toMap
    echo(s"Effects: ${effects}")
    
    verbose(s"Random seed is: ${randomSeed}, sparkPar is: ${sparkPar}")

    echo(s"Loading parquet file: ${inputFile}")
    val featureSource = new ParquetFeatureSource(inputFile)
    echo(s"Loaded rows: ${dumpList(featureSource.sampleNames)}")    
    
    val generator = EfectLabelGenerator(featureSource)(zeroLevel = actualZeroLevel, effects = effects, noiseEffectSigma = noiseSigma, noiseEffectMean = noiseMean, noiseVarFraction = noiseVarFraction, 
        seed = randomSeed)
    echo(s"Saving feature output to: ${featuresFile}, column: ${featureColumn}")    
    
    val labels = generator.getLabels(featureSource.sampleNames)
    
    echo(s"Combined noise mean: ${generator.noiseMean} , sigma: ${generator.noiseSigma}")
    echo(s"Noise variables: ${generator.noiseEffects}")
    //TODO (Refactoring): Refactor to a FeatureSink
    //TODO (Func): Use remote filesystem
    LoanUtils.withCloseable(CSVWriter.open(new File(featuresFile))) { writer =>
      writer.writeRow(List("", featureColumn))
      writer.writeAll(featureSource.sampleNames.zip(labels).map(_.productIterator.toSeq))
    }   
   }
}

object GenerateLabelsCmd  {
  def main(args:Array[String]) {
    AppRunner.mains[GenerateLabelsCmd](args)
  }
}
