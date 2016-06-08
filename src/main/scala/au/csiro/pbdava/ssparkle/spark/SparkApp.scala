package au.csiro.pbdava.ssparkle.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
//import org.bdgenomics.adam.rdd.ADAMContext

trait SparkApp {
  def defaultMasterUrl = "yarn-client"
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  
  lazy val conf = new SparkConf()
    //.setMaster("local")
    //.set("spark.shuffle.memoryFraction", "0.2")
    //.set("spark.storage.memoryFraction", "0.4")
    //.set("spark.yarn.executor.memoryOverhead", "2048")
    .set("spark.driver.maxResultSize","2048")
    //.set("spark.default.parallelism", "256")
  lazy val masterUrl =  if (System.getenv("MASTER") != null) System.getenv("MASTER") else "yarn-client"
  lazy val sc = { println("MASTER: " + masterUrl) ; new SparkContext(conf)}
  lazy val sqlContext = new SQLContext(sc)
  //lazy val ac = new ADAMContext(sc)
}
