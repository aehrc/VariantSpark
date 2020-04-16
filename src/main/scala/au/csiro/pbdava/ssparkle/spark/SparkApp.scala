package au.csiro.pbdava.ssparkle.spark

import au.csiro.pbdava.ssparkle.common.utils.Logging
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}

trait SparkApp extends Logging {
  def defaultMasterUrl: String = "local"
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  def createConf: SparkConf = {
    val conf = new SparkConf(!System.getProperty("sparkle.local", "false").toBoolean)
      .setAppName(getClass.getSimpleName)
    if (conf.contains("spark.master")) conf else conf.setMaster(defaultMasterUrl)
  }

  lazy val conf: SparkConf = createConf

  implicit lazy val spark: SparkSession = {
    logDebug("Spark conf: " + conf.toDebugString)
    SparkSession.builder.config(conf).getOrCreate()
  }

  implicit lazy val sc: SparkContext = spark.sparkContext

  @deprecated
  lazy val sqlContext: SQLContext = spark.sqlContext
}
