package au.csiro.variantspark.api

import org.junit.Test
import org.junit.Assert._
import au.csiro.variantspark.api._
import au.csiro.variantspark.algo.metrics.ManhattanPairwiseMetric
import au.csiro.variantspark.algo.metrics.EuclideanPairwiseMetric
import au.csiro.variantspark.algo.metrics.SharedAltAlleleCount
import au.csiro.variantspark.algo.metrics.AtLeastOneSharedAltAlleleCount
import au.csiro.variantspark.test.SparkTest
import org.saddle.io.CsvFile
import org.saddle.io.CsvParser

class CommonPairwiseOperationTest extends SparkTest {

  @Test
  def canResolveCommonNames() {
    assertEquals(ManhattanPairwiseMetric, CommonPairwiseOperation.withName("manhattan"))
    assertEquals(EuclideanPairwiseMetric, CommonPairwiseOperation.withName("euclidean"))
    assertEquals(SharedAltAlleleCount, CommonPairwiseOperation.withName("sharedAltAlleleCount"))
    assertEquals(
      AtLeastOneSharedAltAlleleCount,
      CommonPairwiseOperation.withName("anySharedAltAlleleCount"))
  }

  @Test
  def testManhattanPaiwiseOperation() {
    implicit val vsContext = VSContext(spark)
    val features = vsContext.importCSV("src/test/data/synthetic_100x10k.csv");
    val result = features.pairwiseOperation("manhattan").value
    val expected = CsvParser
      .parse(CsvFile("src/test/data/synthetic_100x10k_metrics.csv"))
      .withRowIndex(0)
      .withColIndex(0)
      .firstCol("manhattan")
      .mapValues(CsvParser.parseDouble)
      .values
      .toSeq
      .toArray
    assertArrayEquals(expected, result, 1e-5)
  }

  @Test
  def testEuclideanPaiwiseOperation() {
    implicit val vsContext = VSContext(spark)
    val features = vsContext.importCSV("src/test/data/synthetic_100x10k.csv");
    val result = features.pairwiseOperation("euclidean").value
    val expected = CsvParser
      .parse(CsvFile("src/test/data/synthetic_100x10k_metrics.csv"))
      .withRowIndex(0)
      .withColIndex(0)
      .firstCol("euclidean")
      .mapValues(CsvParser.parseDouble)
      .values
      .toSeq
      .toArray
    assertArrayEquals(expected, result, 1e-5)
  }

  @Test
  def testAnySharedAltCountPaiwiseOperation() {
    implicit val vsContext = VSContext(spark)
    val features = vsContext.importCSV("src/test/data/synthetic_100x10k.csv");
    val result = features.pairwiseOperation("anySharedAltAlleleCount").value
    val expected = CsvParser
      .parse(CsvFile("src/test/data/synthetic_100x10k_metrics.csv"))
      .withRowIndex(0)
      .withColIndex(0)
      .firstCol("anySharedCount")
      .mapValues(CsvParser.parseDouble)
      .values
      .toSeq
      .toArray
    assertArrayEquals(expected, result, 1e-5)
  }

  @Test
  def testAllSharedAltCountPaiwiseOperation() {
    implicit val vsContext = VSContext(spark)
    val features = vsContext.importCSV("src/test/data/synthetic_100x10k.csv");
    val result = features.pairwiseOperation("sharedAltAlleleCount").value
    val expected = CsvParser
      .parse(CsvFile("src/test/data/synthetic_100x10k_metrics.csv"))
      .withRowIndex(0)
      .withColIndex(0)
      .firstCol("allSharedCount")
      .mapValues(CsvParser.parseDouble)
      .values
      .toSeq
      .toArray
    assertArrayEquals(expected, result, 1e-5)
  }
}
