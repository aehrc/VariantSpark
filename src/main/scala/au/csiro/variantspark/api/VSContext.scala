package au.csiro.variantspark.api

import au.csiro.variantspark.input.{
  CsvLabelSource,
  FeatureSource,
  VCFFeatureSource,
  VCFSource,
  CsvFeatureSource,
  CsvStdFeatureSource,
  UnionedFeatureSource
}
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext
import com.github.tototoshi.csv.CSVFormat
import au.csiro.variantspark.input.DefaultCSVFormatSpec
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scala.collection.JavaConverters._

trait SqlContextHolder {
  def sqlContext: SQLContext
}

/** The main entry point for VariantSpark API
  * @param spark SparkSession to use
  */
class VSContext(val spark: SparkSession) extends SqlContextHolder {

  val sc: SparkContext = spark.sparkContext
  val sqlContext: SQLContext = spark.sqlContext

  implicit val fs: FileSystem = FileSystem.get(sc.hadoopConfiguration)
  implicit val hadoopConf: Configuration = sc.hadoopConfiguration

  /** Import features from a VCF file
    * @param inputFile path to file or directory with VCF files to load
    * @return FeatureSource loaded from the VCF file
    */
  def importVCF(inputFile: String, imputationStrategy: String = "none"): FeatureSource = {
    val vcfSource =
      VCFSource(sc, inputFile)
    //  VCFSource(sc.textFile(inputFile, if (sparkPar > 0) sparkPar else sc.defaultParallelism))
    VCFFeatureSource(vcfSource, imputationStrategy)
  }

  /** Import features from a CSV file
    * @param inputFile: path to CSV file
    * @param optVariableTypes: optional type specifications
    * @param csvFormat: [[com.github.tototoshi.csv.CSVFormat]] row format
    * @return FeatureSource loaded from the CSV file
    */
  def importTransposedCSV(inputFile: String,
      optVariableTypes: Option[RDD[String]], csvFormat: CSVFormat): FeatureSource = {
    CsvFeatureSource(sc.textFile(inputFile), csvFormat = csvFormat,
      optVariableTypes = optVariableTypes)
  }
  def importTransposedCSV(inputFile: String,
      variableTypes: java.util.ArrayList[String] = null): FeatureSource = {
    val csvFormat: CSVFormat = DefaultCSVFormatSpec
    val optVariableTypes: Option[RDD[String]] = Option(variableTypes).map { types =>
      sc.parallelize(types.asScala.toSeq)
    }
    importTransposedCSV(inputFile, optVariableTypes, csvFormat)
  }
  def importTransposedCSV(inputFile: String): FeatureSource = {
    val csvFormat: CSVFormat = DefaultCSVFormatSpec
    val optVariableTypes: Option[RDD[String]] = None
    importTransposedCSV(inputFile, optVariableTypes, csvFormat)
  }

  /** Import features from a transposed CSV file
    * @param inputFile: path to CSV file
    * @param optVariableTypes: optional type specifications
    * @param csvFormat: [[com.github.tototoshi.csv.CSVFormat]] row format
    * @return FeatureSource loaded from CSV file
    */
  def importStdCSV(inputFile: String,
      optVariableTypes: Option[RDD[String]], csvFormat: CSVFormat): FeatureSource = {
    CsvStdFeatureSource(sc.textFile(inputFile), csvFormat = csvFormat,
      optVariableTypes = optVariableTypes)
  }
  def importStdCSV(inputFile: String,
      variableTypes: java.util.ArrayList[String] = null): FeatureSource = {
    val csvFormat: CSVFormat = DefaultCSVFormatSpec
    val optVariableTypes: Option[RDD[String]] = Option(variableTypes).map { types =>
      sc.parallelize(types.asScala.toSeq)
    }
    importStdCSV(inputFile, optVariableTypes, csvFormat)
  }
  def importStdCSV(inputFile: String): FeatureSource = {
    val csvFormat: CSVFormat = DefaultCSVFormatSpec
    val optVariableTypes: Option[RDD[String]] = None
    importStdCSV(inputFile, optVariableTypes, csvFormat)
  }

  /** Combine FeatureSource objects (typically a genotype source and a covariate source)
    * @param featureSource: FeatureSource object containing genotype information
    * @param covariateSource: FeatureSource object containing covariate information
    */
  def unionFeaturesAndCovariates(featureSource: FeatureSource,
      covariateSource: FeatureSource): FeatureSource = {
    UnionedFeatureSource(featureSource, covariateSource)
  }

  /** Loads a labels form a column in a CSV file
    * @param featuresFile path to CSV file with labels
    * @param featureColumn the name of the column to load as the label
    *
    * @return LabelSource loaded from the column of the CSV file
    */
  def loadLabel(featuresFile: String, featureColumn: String): CsvLabelSource = {
    new CsvLabelSource(featuresFile, featureColumn)
  }

  @deprecated
  def featureSource(inputFile: String, inputType: String = null): FeatureSource =
    importVCF(inputFile)

  @deprecated
  def labelSource: (String, String) => CsvLabelSource = loadLabel
}

object VSContext {
  def apply(spark: SparkSession): VSContext = new VSContext(spark)
}
