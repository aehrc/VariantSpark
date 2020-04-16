package au.csiro.variantspark.api

import au.csiro.variantspark.input.{CsvLabelSource, FeatureSource, VCFFeatureSource, VCFSource}
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext
import au.csiro.variantspark.input.CsvFeatureSource
import au.csiro.variantspark.input.CsvFeatureSource._
import com.github.tototoshi.csv.CSVFormat
import au.csiro.variantspark.input.DefaultCSVFormatSpec
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext

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
    * @return FeatureSource loaded from the VCF file or files
    */
  def importVCF(inputFile: String, sparkPar: Int = 0): FeatureSource = {
    val vcfSource =
      VCFSource(sc.textFile(inputFile, if (sparkPar > 0) sparkPar else sc.defaultParallelism))
    VCFFeatureSource(vcfSource)
  }

  /** Import features from a CSV file
    * @param inputFile: path to file or directory with VCF files to load
    * @param csvFormat: [[com.github.tototoshi.csv.CSVFormat]] row format
    * @return FeatureSource loaded from the VCF file or files
    */
  def importCSV(inputFile: String, csvFormat: CSVFormat = DefaultCSVFormatSpec): FeatureSource = {
    CsvFeatureSource(sc.textFile(inputFile), csvFormat = csvFormat)
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
