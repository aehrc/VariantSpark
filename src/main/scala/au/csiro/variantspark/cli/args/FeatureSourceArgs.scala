package au.csiro.variantspark.cli.args

import org.kohsuke.args4j.Option
import au.csiro.variantspark.cmd.Echoable
import au.csiro.variantspark.input.VCFSource
import au.csiro.variantspark.input.VCFFeatureSource
import au.csiro.variantspark.input.CsvFeatureSource
import au.csiro.variantspark.input.ParquetFeatureSource


trait FeatureSourceArgs extends Object with SparkArgs with Echoable  {
  
  // input options
  @Option(name="-if", required=false, usage="Path to input file or directory", aliases=Array("--input-file"))
  val inputFile:String = null

  @Option(name="-it", required=false, usage="Input file type, one of: vcf, csv, parquet (def=vcf)", aliases=Array("--input-type"))
  val inputType:String = "vcf"
  
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
  
  lazy val featureSource = {
      val fileLoader = inputType match {
      case "csv" =>  loadCSV _
      case "parquet" => loadParquet _ 
      case "vcf" => loadVCF _
    }
    fileLoader()
  }
 
}