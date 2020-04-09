package au.csiro.variantspark.input

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.junit.Assert._
import org.junit.Test
import au.csiro.variantspark.test.SparkTest
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import au.csiro.variantspark.data.StdFeature
import au.csiro.variantspark.data.ContinuousVariable
import au.csiro.variantspark.data.VectorData

class CsvStdFeatureSourceTest extends SparkTest {

  @Test
  def testConvetsCSVCorectly() {

    val testCSV = sc.parallelize(
      Seq(
        ",var0,var1,var2,var3,var4",
        "s1,0,1,2,3,4",
        "s2,1,2,3,4,5",
        "s3,2,3,4,5,6",
        "s4,3,4,5,6,7",
        "s5,4,5,6,7,8",
        "s6,5,6,7,8,9"),
      3)

    val featureSource = CsvStdFeatureSource(testCSV)
    val features = featureSource.featuresAs[Vector].collect()
    assertEquals((1 to 6).map("s" + _).toList, featureSource.sampleNames)
    (0 to 4).foreach { i =>
      assertEquals(
        StdFeature(
          "var" + i,
          ContinuousVariable,
          VectorData(Vectors.dense((i to (i + 5)).map(_.toDouble).toArray))),
        features(i))
    }
  }

  @Test
  def testConvertsLargeFileCorectly() {
    val featureSource = CsvStdFeatureSource(sc.textFile("data/har_aal.csv.bz2", 5))
    assertEquals((0 to 4251).map(_.toString), featureSource.sampleNames)
    val features = featureSource.featuresAs[Vector].collect()
    val featuresByNames = features.map(f => (f.label, f)).toMap
    val expectedVariableNames = (0 to 560).map(_.toString).sorted.toList
    assertEquals(expectedVariableNames, features.map(_.label).toList)
    // check a few random values
    assertEquals(-0.50893, featuresByNames("5").data.at(24), 1e-7)
    assertEquals(0.40626, featuresByNames("558").data.at(4246), 1e-7)
  }
}
