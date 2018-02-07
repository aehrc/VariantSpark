package au.csiro.pbdava.ssparkle.spark

import au.csiro.pbdava.ssparkle.common.utils.Logging
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait SparkApp extends Logging {
  def defaultMasterUrl = "local"
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  
  def createConf: SparkConf =  {
      val conf = new SparkConf(!System.getProperty("sparkle.local","false").toBoolean)
        .setAppName(getClass().getSimpleName)
      if (conf.contains("spark.master")) conf else conf.setMaster(defaultMasterUrl) 
  } 
  
  lazy val conf = createConf

  implicit lazy val spark = {
    logDebug("Spark conf: " + conf.toDebugString)
    SparkSession.builder.config(conf).getOrCreate()
  }
  
  @deprecated
  implicit lazy val sc = spark.sparkContext
  
  @deprecated
  lazy val sqlContext = spark.sqlContext
}
