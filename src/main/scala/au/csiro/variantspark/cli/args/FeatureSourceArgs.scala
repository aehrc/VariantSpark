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


trait FeatureSourceArgs extends Object with SparkArgs with Echoable  {

  @Option(name="-if", required=false, usage="Path to input file or directory", aliases=Array("--input-file"))
  val inputFile:String = null

  @Option(name="-it", required=false, usage="Input file type, one of: vcf, csv, parquet (def=vcf)", aliases=Array("--input-type"))
  val inputType:String = "vcf"

  @Option(name="-ivb", required=false, usage="Input vcf is biallelic (def=false)", aliases=Array("--input-vcf-biallelic"))
  val inputVcfBiallelic:Boolean = false

  @Option(name="-ivs", required=false, usage="The separator to use to produce labels for variants from vcf file.(def='_')", aliases=Array("--input-vcf-sep"))
  val inputVcfSeparator:String = "_"
  
  def loadVCF() = {
    echo(s"Loading header from VCF file: ${inputFile}")
    val vcfSource = VCFSource(sc.textFile(inputFile, if (sparkPar > 0) sparkPar else sc.defaultParallelism))
    verbose(s"VCF Version: ${vcfSource.version}")
    verbose(s"VCF Header: ${vcfSource.header}")    
    VCFFeatureSource(vcfSource, inputVcfBiallelic, inputVcfSeparator)
  }
  
  def loadCSV() = {
    echo(s"Loading csv file: ${inputFile}")
    CsvFeatureSource[Array[Byte]](sc.textFile(inputFile, if (sparkPar > 0) sparkPar else sc.defaultParallelism))
  }
  
  def loadParquet() = {
    echo(s"Loading parquet file: ${inputFile}")
    ParquetFeatureSource(inputFile)
  }
  
  def creatreFeatureSource[R]:FeatureSource = {
      val fileLoader = inputType match {
      case "csv" =>  loadCSV _
      case "parquet" => loadParquet _ 
      case "vcf" => loadVCF _
    }
    fileLoader()    
  }
  
  lazy val featureSource = {
      val fileLoader = inputType match {
      case "csv" =>  loadCSV _
      case "parquet" => loadParquet _ 
      case "vcf" => loadVCF _
    }
    fileLoader()
  }
 
  def echoDataPreview() {
    if (isVerbose) {
      verbose("Data preview:")
      //TODO:FIX
      //featureSource.features().take(defaultPreviewSize).foreach(f=> verbose(s"${f.label}:${dumpList(f.values.toList, longPreviewSize)}"))
    }  
  }
  
}