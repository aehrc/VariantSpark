package au.csiro.variantspark.api

import au.csiro.variantspark.algo.metrics.{
  AtLeastOneSharedAltAlleleCount,
  EuclideanPairwiseMetric,
  ManhattanPairwiseMetric,
  SharedAltAlleleCount
}
import au.csiro.variantspark.test.{SparkTest, TestCsvUtils}
import org.junit.Assert._
import org.junit.Test

class CommonPairwiseOperationTest extends SparkTest {

  @Test
  def canResolveCommonNames() {
    assertEquals(ManhattanPairwiseMetric, CommonPairwiseOperation.withName("manhattan"))
    assertEquals(EuclideanPairwiseMetric, CommonPairwiseOperation.withName("euclidean"))
    assertEquals(SharedAltAlleleCount, CommonPairwiseOperation.withName("sharedAltAlleleCount"))
    assertEquals(AtLeastOneSharedAltAlleleCount,
      CommonPairwiseOperation.withName("anySharedAltAlleleCount"))
  }

  @Test
  def testManhattanPaiwiseOperation() {
    implicit val vsContext = VSContext(spark)
    val features = vsContext.importTransposedCSV("src/test/data/synthetic_100x10k.csv");
    val result = features.pairwiseOperation("manhattan").value
    val expected = TestCsvUtils.readColumnToDoubleArray(
        "src/test/data/synthetic_100x10k_metrics.csv", "manhattan")
    assertArrayEquals(expected, result, 1e-5)
  }

  @Test
  def testEuclideanPaiwiseOperation() {
    implicit val vsContext = VSContext(spark)
    val features = vsContext.importTransposedCSV("src/test/data/synthetic_100x10k.csv");
    val result = features.pairwiseOperation("euclidean").value
    val expected = TestCsvUtils.readColumnToDoubleArray(
        "src/test/data/synthetic_100x10k_metrics.csv", "euclidean")
    assertArrayEquals(expected, result, 1e-5)
  }

  @Test
  def testAnySharedAltCountPaiwiseOperation() {
    implicit val vsContext = VSContext(spark)
    val features = vsContext.importTransposedCSV("src/test/data/synthetic_100x10k.csv");
    val result = features.pairwiseOperation("anySharedAltAlleleCount").value
    val expected = TestCsvUtils.readColumnToDoubleArray(
        "src/test/data/synthetic_100x10k_metrics.csv", "anySharedCount")
    assertArrayEquals(expected, result, 1e-5)
  }

  @Test
  def testAllSharedAltCountPaiwiseOperation() {
    implicit val vsContext = VSContext(spark)
    val features = vsContext.importTransposedCSV("src/test/data/synthetic_100x10k.csv");
    val result = features.pairwiseOperation("sharedAltAlleleCount").value
    val expected = TestCsvUtils.readColumnToDoubleArray(
        "src/test/data/synthetic_100x10k_metrics.csv", "allSharedCount")
    assertArrayEquals(expected, result, 1e-5)
  }
}
