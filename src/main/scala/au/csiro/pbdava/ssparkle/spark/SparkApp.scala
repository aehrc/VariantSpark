package au.csiro.pbdava.ssparkle.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.Logging

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
    //.set("spark.shuffle.memoryFraction", "0.2")
    //.set("spark.storage.memoryFraction", "0.4")
    //.set("spark.yarn.executor.memoryOverhead", "2048")
    //.set("spark.driver.maxResultSize","2048")
    //.set("spark.default.parallelism", "256")
  implicit lazy val sc = {
    logDebug("Spark conf: " + conf.toDebugString)
    new SparkContext(conf)
  }
  lazy val sqlContext = new SQLContext(sc)
}
