package au.csiro.variantspark.test

trait SparkTest {
  implicit lazy val spark = TestSparkContext.spark
  implicit lazy val sc = spark.sparkContext
}