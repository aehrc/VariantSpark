package au.csiro.variantspark.input

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.junit.Assert._
import org.junit.Test
import au.csiro.variantspark.test.SparkTest
import org.apache.spark.mllib.linalg.{Vector,Vectors}


class CsvStdFeatureSourceTest extends SparkTest {
  
  @Test
  def testConvetsCSVCorectly() {
    
    val testCSV = sc.parallelize(Seq(
        ",var0,var1,var2,var3,var4",
        "s1,0,1,2,3,4",
        "s2,0,1,3,3,4",
        "s3,0,1,2,3,4",
        "s4,0,1,2,3,4",
        "s5,0,1,2,3,4",
        "s6,0,1,2,3,4",
        "s7,0,1,2,3,4"
        ),2)
    
    val featureSource = CsvStdFeatureSource(testCSV)
    val features = featureSource.featuresAs[Vector]
    println(features.collect.toList)
    println(featureSource.sampleNames)    
  }

  @Test
  def testConvertsLargeCorectly() {
    val featureSource = CsvStdFeatureSource(sc.textFile("data/har_aal.csv.bz2", 5))
    println(featureSource.sampleNames)          
    val features = featureSource.featuresAs[Vector]
    features.count()
  }
}