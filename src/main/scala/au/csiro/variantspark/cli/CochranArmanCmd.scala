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
import au.csiro.variantspark.input.ParquetFeatureSource
import au.csiro.variantspark.algo.ByteRandomForest
import au.csiro.variantspark.utils.IndexedRDDFunction._
import au.csiro.variantspark.stats.CochranArmitageTestScorer
import au.csiro.variantspark.stats.CochranArmitageTestCalculator
import au.csiro.variantspark.utils.HdfsPath
import au.csiro.pbdava.ssparkle.common.utils.CSVUtils

class CochranArmanCmd extends ArgsApp with SparkApp with Echoable with Logging with TestArgs {

  @Option(name="-if", required=true, usage="Path to input file or directory", aliases=Array("--input-file"))
  val inputFile:String = null

  @Option(name="-it", required=false, usage="Input file type, one of: vcf, csv, parquet (def=vcf)", aliases=Array("--input-type"))
  val inputType:String = "vcf"
  
  @Option(name="-ivo", required=false, usage="Variable type ordinal with this number of levels (def = 3)" 
      , aliases=Array("--input-var-ordinal"))
  val varOrdinalLevels:Int = 3

  @Option(name="-ff", required=true, usage="Path to feature file", aliases=Array("--feature-file"))
  val featuresFile:String = null

  @Option(name="-fc", required=true, usage="Name of the feature column", aliases=Array("--feature-column"))
  val featureColumn:String = null
  

  @Option(name="-of", required=false, usage="Path to output file (def = stdout)", aliases=Array("--output-file") )
  val outputFile:String = null

  @Option(name="-on", required=false, usage="The number of top important variables to include in output (def=20)",
      aliases=Array("--output-n-variables"))
  val nVariables:Int = 20
 
  @Option(name="-od", required=false, usage="Include important variables data in output file (def=no)"
        , aliases=Array("--output-include-data") )
  val includeData = false

  @Option(name="-sp", required=false, usage="Spark parallelism (def=<default-spark-par>)", aliases=Array("--spark-par"))
  val sparkPar = 0
 
  @Override
  def testArgs = Array("-if", "target/getds.parquet", "-it", "parquet", "-ff", "target/features.csv", "-fc", "resp", "-od")
  
  def loadVCF() = {
    echo(s"Loading header from VCF file: ${inputFile}")
    val vcfSource = VCFSource(sc.textFile(inputFile, if (sparkPar > 0) sparkPar else sc.defaultParallelism))
    verbose(s"VCF Version: ${vcfSource.version}")
    verbose(s"VCF Header: ${vcfSource.header}")    
    VCFFeatureSource(vcfSource)    
  }
  
  def loadCSV() = {
    echo(s"Loading csv file: ${inputFile}")
    CsvFeatureSource(sc.textFile(inputFile, if (sparkPar > 0) sparkPar else sc.defaultParallelism))
  }
  
  def loadParquet() = {
    echo(s"Loading parquet file: ${inputFile}")
    ParquetFeatureSource(inputFile)
  }
  
  @Override
  def run():Unit = {
    implicit val fs = FileSystem.get(sc.hadoopConfiguration)      
    implicit val hadoopConf = sc.hadoopConfiguration
    
    logDebug(s"Running with filesystem: ${fs}, home: ${fs.getHomeDirectory}")
    logInfo("Running with params: " + ToStringBuilder.reflectionToString(this))
    echo(s"Finding  ${nVariables}  most important features using random forest")

    // TODO (Refactoring): Make a class
    // TODO (Func): add auto detection based on name
    val fileLoader = inputType match {
      case "csv" =>  loadCSV _
      case "parquet" => loadParquet _ 
      case "vcf" => loadVCF _
    }
    val source = fileLoader()
    
    echo(s"Loaded rows: ${dumpList(source.sampleNames)}")
     
    echo(s"Loading labels from: ${featuresFile}, column: ${featureColumn}")
    val labelSource = new CsvLabelSource(featuresFile, featureColumn)
    val labels = labelSource.getLabels(source.sampleNames)
    echo(s"Loaded labels: ${dumpList(labels.toList)}")
    
    
    val dataLoadingTimer = Timer()
    echo(s"Loading features from: ${inputFile}")
    
    val inputData = source.features().zipWithIndex().cache()
    val totalVariables = inputData.count()
    val variablePreview = inputData.map({case (f,i) => f.label}).take(defaultPreviewSize).toList
        
    echo(s"Loaded variables: ${dumpListHead(variablePreview, totalVariables)}, took: ${dataLoadingTimer.durationInSec}")
    
    // discover variable type
    // for now assume it's ordered factor with provided number of levels
    echo(s"Assumed ordinal variable with ${varOrdinalLevels} levels")
    // TODO (Feature): Add autodiscovery
    val dataType = BoundedOrdinal(varOrdinalLevels)
    
    if (isVerbose) {
      verbose("Data preview:")
      source.features().take(defaultPreviewSize).foreach(f=> verbose(s"${f.label}:${dumpList(f.values.toList, longPreviewSize)}"))
    }
    
    echo(s"Running two sided CochranArmitage test")  
     val trainingData = inputData.map{ case (f, i) => (f.values, i)}
    
    
    val scorer = new CochranArmitageTestScorer(labels, CochranArmitageTestCalculator.WEIGHT_TREND, nVariables)
    val topImportantVariables = scorer.topN(trainingData)
    val topImportantVariableIndexes = topImportantVariables.map(_._1).toSet
    
    val index = SparkUtils.withBroadcast(sc)(topImportantVariableIndexes) { br_indexes => 
      inputData.filter(t => br_indexes.value.contains(t._2)).map({case (f,i) => (i, f.label)}).collectAsMap()
    }
    
    val varImportance = topImportantVariables.map({ case (i, importance) => (index(i), importance)})
    
    if (isEcho && outputFile!=null) {
      echo("Variable importance preview")
      varImportance.take(math.min(nVariables, defaultPreviewSize)).foreach({case(label, importance) => echo(s"${label}: ${importance}")})
    }

    val importantVariableData = if (includeData) trainingData.collectAtIndexes(topImportantVariableIndexes) else null
    
    CSVUtils.withStream(if (outputFile != null ) HdfsPath(outputFile).create() else ReusablePrintStream.stdout) { writer =>
      val header = List("variable","importance") ::: (if (includeData) source.sampleNames else Nil)
      writer.writeRow(header)
      writer.writeAll(topImportantVariables.map({case (i, importance) => 
        List(index(i), importance) ::: (if (includeData) importantVariableData(i).toArray.toList else Nil)}))
    }
  }
}

object CochranArmanCmd  {
  def main(args:Array[String]) {
    AppRunner.mains[CochranArmanCmd](args)
  }
}
