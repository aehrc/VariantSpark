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
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.rdd.RDD
import au.csiro.variantspark.data.Feature

trait FeatureSourceFactory {
  def createSource(sparkArgs: SparkArgs): FeatureSource
}

object VCFFeatureSourceFactory {
  val DEF_IS_BIALLELIC: Boolean = false
  val DEF_SEPARATOR: String = "_"
}

case class VCFFeatureSourceFactory(inputFile: String, isBiallelic: Option[Boolean],
    separator: Option[String])
    extends FeatureSourceFactory with Echoable {
  def createSource(sparkArgs: SparkArgs): FeatureSource = {
    echo(s"Loading header from VCF file: ${inputFile}")
    val vcfSource = VCFSource(sparkArgs.textFile(inputFile))
    verbose(s"VCF Version: ${vcfSource.version}")
    verbose(s"VCF Header: ${vcfSource.header}")

    import VCFFeatureSourceFactory._
    VCFFeatureSource(vcfSource, isBiallelic.getOrElse(DEF_IS_BIALLELIC),
      separator.getOrElse(DEF_SEPARATOR))
  }
}

object CSVFeatureSourceFactory {
  val DEF_VARIABLE_TYPE: ContinuousVariable.type = ContinuousVariable
}

case class CSVFeatureSourceFactory(inputFile: String, defVariableType: Option[String],
    variableTypeFile: Option[String])
    extends FeatureSourceFactory with Echoable {
  import CSVFeatureSourceFactory._
  def createSource(sparkArgs: SparkArgs): FeatureSource = {
    echo(s"Loading csv file: ${inputFile}")
    val inputVariableType =
      defVariableType.map(VariableType.fromString).getOrElse(DEF_VARIABLE_TYPE)
    echo(
        s"Default input variable type is ${inputVariableType},"
          + s" variable type file is ${variableTypeFile}")
    val typeRDD = variableTypeFile.map(fileName => sparkArgs.sc.textFile(fileName))
    val dataRDD = sparkArgs.textFile(inputFile)
    CsvFeatureSource(dataRDD, inputVariableType, typeRDD)
  }
}

case class ParquetFeatureSourceFactory(inputFile: String)
    extends FeatureSourceFactory with Echoable {
  def createSource(sparkArgs: SparkArgs): FeatureSource = {
    import sparkArgs._
    echo(s"Loading parquet file: ${inputFile}")
    ParquetFeatureSource(inputFile)
  }
}

case class StdCSVFeatureSourceFactory(inputFile: String)
    extends FeatureSourceFactory with Echoable {
  def createSource(sparkArgs: SparkArgs): FeatureSource = {
    import sparkArgs._
    echo(s"Loading standard csv file: ${inputFile}")
    CsvStdFeatureSource[Array[Byte]](textFile(inputFile))
  }
}

class CompositeFeatueSource(featureSources: Seq[FeatureSource]) extends FeatureSource {
  require(featureSources.nonEmpty)
  override lazy val sampleNames: List[String] = {
    val headSampleNames = featureSources.head.sampleNames
    require(featureSources.tail.forall(_.sampleNames == headSampleNames))
    headSampleNames
  }

  def features: RDD[Feature] = {
    // Make Union of all samples
    // this does not check for duplicate variable though
    val compositeRDDs = featureSources.map(_.features)
    compositeRDDs.reduceLeft(_.union(_))
  }
}

case class CompositeFeatureSourceFactory(sourceFactories: Seq[FeatureSourceFactory])
    extends FeatureSourceFactory {
  def createSource(sparkArgs: SparkArgs): FeatureSource =
    new CompositeFeatueSource(sourceFactories.map(_.createSource(sparkArgs)))
}

trait FeatureSourceArgs extends Object with SparkArgs with Echoable {

  import org.json4s._
  import org.json4s.jackson.JsonMethods._
  import org.json4s.JsonDSL._
  implicit val formats: DefaultFormats.type = DefaultFormats

  @ArgsOption(name = "-if", required = false, usage = "Path to input file or directory",
    aliases = Array("--input-file"))
  val inputFile: String = null

  @ArgsOption(name = "-it", required = false,
    usage = "Input file type, one of: vcf, csv, parquet (def=vcf)",
    aliases = Array("--input-type"))
  val inputType: String = "vcf"

  @ArgsOption(name = "-io", required = false,
    usage = "a JSON object with the additional options for the input file type "
      + "(depends on input file type)",
    aliases = Array("--input-options"))
  val inputOptions: String = null

  @ArgsOption(name = "-ij", required = false, usage = "Input JSON specification",
    aliases = Array("--input-json"))
  val inputJSON: String = null

  def featureSourceFactory(inputJSON: JValue): FeatureSourceFactory = {
    inputJSON match {
      case jsonObject: JObject => featureSourceFactory(jsonObject)
      case jsonArray: JArray => featureSourceFactory(jsonArray)
      case _ =>
        throw new IllegalArgumentException(
            s"Cannot create source from spec of class ${inputJSON.getClass}")
    }
  }

  def featureSourceFactory(inputJSON: JArray): FeatureSourceFactory = {
    val sourceFactories = inputJSON.children.map(featureSourceFactory)
    CompositeFeatureSourceFactory(
        sourceFactories.foldLeft(ArrayBuffer[FeatureSourceFactory]())(_ += _))
  }

  def featureSourceFactory(inputJSON: JObject): FeatureSourceFactory = {
    verbose(s"Input Spec is : ${inputJSON}")
    val inputType = inputJSON \ "type"
    inputType match {
      case JString("csv") => inputJSON.extract[CSVFeatureSourceFactory]
      case JString("stdcsv") => inputJSON.extract[StdCSVFeatureSourceFactory]
      case JString("parquet") => inputJSON.extract[ParquetFeatureSourceFactory]
      case JString("vcf") => inputJSON.extract[VCFFeatureSourceFactory]
      case _ => throw new IllegalArgumentException(s"Unrecognized input type: ${inputType}")
    }
  }

  def featureSourceFactory: FeatureSourceFactory = {
    val inputSpec: JValue = if (inputJSON != null) {
      parse(inputJSON)
    } else {
      val inputOptionsObject =
        Option(inputOptions).map(parse(_).asInstanceOf[JObject]).getOrElse(JObject())
      inputOptionsObject ~ ("inputFile", inputFile) ~ ("type", inputType)
    }
    featureSourceFactory(inputSpec)
  }

  lazy val featureSource: FeatureSource = featureSourceFactory.createSource(this)
  def echoDataPreview() {
    if (isVerbose) {
      verbose("Data preview:")
      featureSource.features
        .take(defaultPreviewSize)
        .foreach(
            f =>
              verbose(
                  s"${f.label}:${f.variableType}:"
                    + s"${dumpList(f.valueAsStrings, longPreviewSize)}"
                    + s"(${f.getClass.getName})"))
    }
  }

}
