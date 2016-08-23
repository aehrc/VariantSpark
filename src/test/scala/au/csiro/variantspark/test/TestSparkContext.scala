package au.csiro.variantspark.test

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object TestSparkContext {
  lazy val sc = new SparkContext(new SparkConf(false).setAppName("test").setMaster("local"))
}