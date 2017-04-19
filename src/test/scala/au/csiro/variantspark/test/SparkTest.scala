package au.csiro.variantspark.test

import org.apache.hadoop.fs.FileSystem

trait SparkTest {
  implicit lazy val spark = TestSparkContext.spark
  implicit lazy val sc = spark.sparkContext
}