package au.csiro.variantspark.cli.args

import org.kohsuke.args4j.{Option => ArgsOption}
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
  val DEF_IS_BIALLELIC = false
  val DEF_SEPARATOR = "_"
}

case class VCFFeatureSourceFactory(inputFile:String, isBiallelic:Option[Boolean], separator:Option[String]) extends FeatureSourceFactory with Echoable {
  def createSource(sparkArgs:SparkArgs): FeatureSource  = {
    echo(s"Loading header from VCF file: ${inputFile}")
    val vcfSource = VCFSource(sparkArgs.textFile(inputFile))
    verbose(s"VCF Version: ${vcfSource.version}")
    verbose(s"VCF Header: ${vcfSource.header}")   
    
    import VCFFeatureSourceFactory._
    VCFFeatureSource(vcfSource, 
        isBiallelic.getOrElse(DEF_IS_BIALLELIC),
        separator.getOrElse(DEF_SEPARATOR))
  }
}

object CSVFeatureSourceFactory {
   val DEF_VARIABLE_TYPE = ContinuousVariable
}

case class CSVFeatureSourceFactory(inputFile:String, defVariableType:Option[String], variableTypeFile:Option[String]) extends FeatureSourceFactory with Echoable {
  import CSVFeatureSourceFactory._
  def createSource(sparkArgs:SparkArgs): FeatureSource  = {
    echo(s"Loading csv file: ${inputFile}")
    val inputVariableType = defVariableType.map(VariableType.fromString).getOrElse(DEF_VARIABLE_TYPE)
    echo(s"Default input variable type is ${inputVariableType}, variable type file is ${variableTypeFile}")
    val typeRDD = variableTypeFile.map(fileName => sparkArgs.sc.textFile(fileName))
    val dataRDD = sparkArgs.textFile(inputFile)
    CsvFeatureSource(dataRDD,inputVariableType, typeRDD)
  }
}

case class ParquetFeatureSourceFactory(inputFile:String) extends FeatureSourceFactory with Echoable {
  def createSource(sparkArgs:SparkArgs): FeatureSource  = {
    import sparkArgs._
    echo(s"Loading parquet file: ${inputFile}")
    ParquetFeatureSource(inputFile)
  }
}

case class StdCSVFeatureSourceFactory(inputFile:String) extends FeatureSourceFactory with Echoable {
  def createSource(sparkArgs:SparkArgs): FeatureSource  = {
    import sparkArgs._
    echo(s"Loading standard csv file: ${inputFile}")
    CsvStdFeatureSource[Array[Byte]](textFile(inputFile))
  }
}

trait FeatureSourceArgs extends Object with SparkArgs with Echoable  {

  @ArgsOption(name="-if", required=false, usage="Path to input file or directory", aliases=Array("--input-file"))
  val inputFile:String = null

  @ArgsOption(name="-it", required=false, usage="Input file type, one of: vcf, csv, parquet (def=vcf)", aliases=Array("--input-type"))
  val inputType:String = "vcf"

  @ArgsOption(name="-io", required=false, usage="a JSON object with the additional options for the input file type (depends on input file type)", aliases=Array("--input-options"))
  val inputOptions:String = null
  
  def featureSourceFactory: FeatureSourceFactory = {
    // parse options
    import org.json4s._
    import org.json4s.jackson.JsonMethods._
    import org.json4s.JsonDSL._
    implicit val formats = DefaultFormats
    
    // extract options from the arguments
    val inputOptionsObject =  Option(inputOptions).map(parse(_).asInstanceOf[JObject]).getOrElse(JObject()) 
    verbose(s"Input JSON options are: ${inputOptions} ->  ${inputOptionsObject}")
    val inputOptionsWithFile = inputOptionsObject ~ ("inputFile", inputFile)
    inputType match {
      case "csv" =>  inputOptionsWithFile.extract[CSVFeatureSourceFactory]
      case "stdcsv" =>   inputOptionsWithFile.extract[StdCSVFeatureSourceFactory]
      case "parquet" => inputOptionsWithFile.extract[ParquetFeatureSourceFactory]
      case "vcf" => inputOptionsWithFile.extract[VCFFeatureSourceFactory]
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