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
import au.csiro.variantspark.data.ContinuousVariable

trait FeatureSourceFactory {
  def createSource(sparkArgs:SparkArgs): FeatureSource
}

object VCFFeatureSourceFactory {
  val KEY_IS_BIALLELIC = "isBiallelic"
  val DEF_IS_BIALLELIC = false
  val KEY_SEPARATOR = "separator"
  val DEF_SEPARATOR = "_"
}

case class VCFFeatureSourceFactory(inputFile:String, options:Map[String, String]) extends FeatureSourceFactory with Echoable {
  def createSource(sparkArgs:SparkArgs): FeatureSource  = {
    echo(s"Loading header from VCF file: ${inputFile}")
    val vcfSource = VCFSource(sparkArgs.textFile(inputFile))
    verbose(s"VCF Version: ${vcfSource.version}")
    verbose(s"VCF Header: ${vcfSource.header}")   
    
    import VCFFeatureSourceFactory._
    VCFFeatureSource(vcfSource, 
        options.get(KEY_IS_BIALLELIC).map(_.toBoolean).getOrElse(DEF_IS_BIALLELIC),
        options.getOrElse(KEY_SEPARATOR, DEF_SEPARATOR))
  }
}

object CSVFeatureSourceFactory {
   val KEY_VARIABLE_TYPE = "defVariableType"
   val DEF_VARIABLE_TYPE = ContinuousVariable
   val KEY_VARIABLE_TYPE_FILE = "variableTypeFile"
}

case class CSVFeatureSourceFactory(inputFile:String, options:Map[String, String]) extends FeatureSourceFactory with Echoable {
  import CSVFeatureSourceFactory._
  def createSource(sparkArgs:SparkArgs): FeatureSource  = {
    echo(s"Loading csv file: ${inputFile}")
    val inputVariableType = options.get(KEY_VARIABLE_TYPE).map(VariableType.fromString).getOrElse(DEF_VARIABLE_TYPE)
    val variableTypeFile = options.get(KEY_VARIABLE_TYPE_FILE)
    echo(s"Default input variable type is ${inputVariableType}, variable type file is ${variableTypeFile}")
    val typeRDD = variableTypeFile.map(fileName => sparkArgs.sc.textFile(fileName))
    val dataRDD = sparkArgs.textFile(inputFile)
    CsvFeatureSource(dataRDD,inputVariableType, typeRDD)
  }
}

case class ParquetFeatureSourceFactory(inputFile:String, options:Map[String, String]) extends FeatureSourceFactory with Echoable {
  def createSource(sparkArgs:SparkArgs): FeatureSource  = {
    import sparkArgs._
    echo(s"Loading parquet file: ${inputFile}")
    ParquetFeatureSource(inputFile)
  }
}

case class StdCSVFeatureSourceFactory(inputFile:String, options:Map[String, String]) extends FeatureSourceFactory with Echoable {
  def createSource(sparkArgs:SparkArgs): FeatureSource  = {
    import sparkArgs._
    echo(s"Loading standard csv file: ${inputFile}")
    CsvStdFeatureSource[Array[Byte]](textFile(inputFile))
  }
}

trait FeatureSourceArgs extends Object with SparkArgs with Echoable  {

  @Option(name="-if", required=false, usage="Path to input file or directory", aliases=Array("--input-file"))
  val inputFile:String = null

  @Option(name="-it", required=false, usage="Input file type, one of: vcf, csv, parquet (def=vcf)", aliases=Array("--input-type"))
  val inputType:String = "vcf"

  @Option(name="-io", required=false, usage="List of input options (depends on input file type)", aliases=Array("--input-options"))
  val inputOptions:String = ""
  
  def featureSourceFactory: FeatureSourceFactory = {
    // parse options
    val options =  inputOptions.split(",").map(_.trim).filter(!_.isEmpty).map(_.split("=")).map(l => (l(0).trim, l(1).trim)).toMap
    inputType match {
      case "csv" =>  CSVFeatureSourceFactory(inputFile, options)
      case "stdcsv" =>   StdCSVFeatureSourceFactory(inputFile, options)
      case "parquet" => ParquetFeatureSourceFactory(inputFile, options)
      case "vcf" => VCFFeatureSourceFactory(inputFile, options)
    }      
  }
  
  lazy val featureSource = featureSourceFactory.createSource(this)
  def echoDataPreview() {
    if (isVerbose) {
      verbose("Data preview:")
      featureSource.features.take(defaultPreviewSize).foreach(f=> verbose(s"${f.label}:${f.variableType}:${dumpList(f.valueAsStrings, longPreviewSize)}(${f.getClass.getName})"))
    }  
  }
  
}