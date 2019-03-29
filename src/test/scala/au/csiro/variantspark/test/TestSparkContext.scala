package au.csiro.variantspark.test

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object TestSparkContext {
  lazy val spark = SparkSession.builder.config(new SparkConf(false)).appName("test").master("local")
    //.config("spark.sql.files.openCostInBytes",53687091200L)
    //.config("spark.sql.files.maxPartitionBytes", 53687091200L)
  .getOrCreate()
}