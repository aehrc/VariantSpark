package au.csiro.variantspark.cli.args

import org.kohsuke.args4j.Option
import au.csiro.variantspark.cmd.Echoable
import au.csiro.variantspark.input.VCFSource
import au.csiro.variantspark.input.VCFFeatureSource
import au.csiro.variantspark.input.CsvFeatureSource
import au.csiro.variantspark.input.CsvFeatureSource._
import au.csiro.variantspark.input.ParquetFeatureSource
import au.csiro.variantspark.cmd.EchoUtils._
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import au.csiro.variantspark.input.FeatureSource
import au.csiro.variantspark.input.CsvStdFeatureSource
import au.csiro.variantspark.data.VariableType


trait FeatureSourceArgs extends Object with SparkArgs with Echoable  {

  @Option(name="-if", required=false, usage="Path to input file or directory", aliases=Array("--input-file"))
  val inputFile:String = null

  @Option(name="-it", required=false, usage="Input file type, one of: vcf, csv, parquet (def=vcf)", aliases=Array("--input-type"))
  val inputType:String = "vcf"

  @Option(name="-ivb", required=false, usage="Input vcf is biallelic (def=false)", aliases=Array("--input-vcf-biallelic"))
  val inputVcfBiallelic:Boolean = false

  @Option(name="-ivs", required=false, usage="The separator to use to produce labels for variants from vcf file.(def='_')", aliases=Array("--input-vcf-sep"))
  val inputVcfSeparator:String = "_"
  
  // input options  
  @Option(name="-ivt", required=false, usage="Input variable type, one of [`ord`, `cont` ] (def =  `ord` or vcf files `cont` otherwise)"
      , aliases=Array("--input-var-type"))
  val inputVariableTypeAsString:String = null

  @Option(name="-ivtf", required=false, usage="Input variable type, one of [`ord`, `cont` ] (def =  `ord` or vcf files `cont` otherwise)"
      , aliases=Array("--input-var-types-file"))
  val inputVariableTypesFile:String = null

  
  
  lazy val inputVariableType:scala.Option[VariableType]  = scala.Option(inputVariableTypeAsString).map(VariableType.fromString _)
      
  
  def loadVCF() = {
    echo(s"Loading header from VCF file: ${inputFile}")
    val vcfSource = VCFSource(sc.textFile(inputFile, if (sparkPar > 0) sparkPar else sc.defaultParallelism))
    verbose(s"VCF Version: ${vcfSource.version}")
    verbose(s"VCF Header: ${vcfSource.header}")    
    VCFFeatureSource(vcfSource, inputVcfBiallelic, inputVcfSeparator)
  }
  
  def loadCSV() = {
    echo(s"Loading csv file: ${inputFile}")
    echo(s"Variable type is ${inputVariableType}")
    val dataRDD = sc.textFile(inputFile, if (sparkPar > 0) sparkPar else sc.defaultParallelism)
    val typeRDD = scala.Option(inputVariableTypesFile).map(fileName => sc.textFile(fileName))
    inputVariableType.map(CsvFeatureSource(dataRDD,_, optVariableTypes=typeRDD)).getOrElse(CsvFeatureSource(dataRDD, optVariableTypes=typeRDD)) 
  }
  
  def loadStdCSV() = {
    echo(s"Loading standard csv file: ${inputFile}")
    CsvStdFeatureSource[Array[Byte]](sc.textFile(inputFile, if (sparkPar > 0) sparkPar else sc.defaultParallelism))
  }
  
  def loadParquet() = {
    echo(s"Loading parquet file: ${inputFile}")
    ParquetFeatureSource(inputFile)
  }
  
  def creatreFeatureSource[R]:FeatureSource = {
      val fileLoader = inputType match {
      case "csv" =>  loadCSV _
      case "stdcsv" =>  loadStdCSV _
      case "parquet" => loadParquet _ 
      case "vcf" => loadVCF _
    }
    fileLoader()    
  }
  
  lazy val featureSource = {
      val fileLoader = inputType match {
      case "csv" =>  loadCSV _
      case "stdcsv" =>  loadStdCSV _
      case "parquet" => loadParquet _ 
      case "vcf" => loadVCF _
    }
    fileLoader()
  }
 
  def echoDataPreview() {
    if (isVerbose) {
      verbose("Data preview:")
      //featureSource.features.take(defaultPreviewSize).foreach(f=> verbose(f.toString))
      featureSource.features.take(defaultPreviewSize).foreach(f=> verbose(s"${f.label}:${f.variableType}:${dumpList(f.valueAsStrings, longPreviewSize)}(${f.getClass.getName})"))
    }  
  }
  
}