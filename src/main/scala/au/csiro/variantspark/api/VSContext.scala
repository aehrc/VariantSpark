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

class VSContext(val spark:SparkSession, val sparkPar:Int=0) extends SqlContextHolder {
  
  val sc = spark.sparkContext
  val sqlContext = spark.sqlContext
  
  implicit val fs = FileSystem.get(sc.hadoopConfiguration)
  implicit val hadoopConf = sc.hadoopConfiguration
  
  def featureSource(inputFile:String, inputType:String = null): FeatureSource = {
    val vcfSource = VCFSource(sc.textFile(inputFile, if (sparkPar > 0) sparkPar else sc.defaultParallelism))
    VCFFeatureSource(vcfSource) 
  }
  
  def labelSource(featuresFile:String, featureColumn:String)  = {
    new CsvLabelSource(featuresFile, featureColumn)
  }
}

object VSContext {
  def apply(spark:SparkSession) = new VSContext(spark)
}

