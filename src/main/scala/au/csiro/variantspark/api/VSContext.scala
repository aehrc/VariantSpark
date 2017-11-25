package au.csiro.variantspark.api

import au.csiro.variantspark.input.{CsvLabelSource, FeatureSource, VCFFeatureSource, VCFSource}
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext

/**
  * A class to represent an instance of the variant-spark context, or spark sql context
  *
  * @constructor Create a new `VSContext` by specifying the `SparkSession` and `sparkPar`
  *
  * @param spark The spark session.
  * @param sparkPar The number of partitions in the the spark session.
  *
  * @example class VSContext(val spark:SparkSession, val sparkPar:Int=0)
  */

trait SqlContextHolder {
  def sqlContext:SQLContext
}


/** The main entry point for VariantSpark API
  * @param spark SparkSession to use
  */
class VSContext(val spark:SparkSession) extends SqlContextHolder {
  
  val sc = spark.sparkContext
  val sqlContext = spark.sqlContext
  
  implicit val fs = FileSystem.get(sc.hadoopConfiguration)
  implicit val hadoopConf = sc.hadoopConfiguration
  
  
  /** Import features from a VCF file
   	* @param inputFile path to file or directory with VCF files to load
   	* @return FeatureSource loaded from the VCF file or files 
   	*/
  def importVCF(inputFile:String):FeatureSource = {
    val vcfSource = VCFSource(sc.textFile(inputFile))
    VCFFeatureSource(vcfSource)     
  }
  
  /** Loads a labels form a column in a CSV file
   	* @param featuresFile path to CSV file with labels
   	* @param featureColumn the name of the column to load as the label
   	* 
   	* @return LabelSource loaded from the column of the CSV file 
   	*/
  def loadLabel(featuresFile:String, featureColumn:String)  = {
		new CsvLabelSource(featuresFile, featureColumn)
	}
  
  @deprecated
  def featureSource(inputFile:String, inputType:String = null): FeatureSource = importVCF(inputFile)
  
  @deprecated
  def labelSource = loadLabel _
}

object VSContext {
  def apply(spark:SparkSession) = new VSContext(spark)
}

