package au.csiro.variantspark.test

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object TestSparkContext {
  lazy val spark = SparkSession.builder.config(new SparkConf(false)).appName("test").master("local").getOrCreate()
}