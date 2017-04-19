package au.csiro.pbdava.ssparkle.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import au.csiro.pbdava.ssparkle.common.utils.Logging
import org.apache.spark.sql.SparkSession

//import org.bdgenomics.adam.rdd.ADAMContext

trait SparkApp extends Logging {
  def defaultMasterUrl = "local"
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  
  lazy val conf = { 
      val conf = new SparkConf(!System.getProperty("sparkle.local","false").toBoolean)
        .setAppName(getClass().getSimpleName)
      if (conf.contains("spark.master")) conf else conf.setMaster(defaultMasterUrl) 
    } 

  implicit lazy val spark = {
    logDebug("Spark conf: " + conf.toDebugString)
    SparkSession.builder.config(conf).getOrCreate()
  }
  
  @deprecated
  implicit lazy val sc = spark.sparkContext
  
  @deprecated
  lazy val sqlContext = spark.sqlContext
}
