package au.csiro.variantspark.algo

import au.csiro.variantspark.data._
import au.csiro.variantspark.test.SparkTest
import it.unimi.dsi.fastutil.longs.{Long2DoubleOpenHashMap, Long2LongOpenHashMap}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.junit.Assert._
import org.junit.Test

class WideRandomForestRegressionModelTest extends SparkTest {
  val doubleComparisonDelta = 1e-6
  val nSamples = 2
  val testData: RDD[(Feature, Long)] =
    sc.parallelize(List(Vectors.zeros(nSamples))).asFeature(BoundedOrdinalVariable(3))

  @Test
  def whenManyPredictorsThenAveragesImportance() {
    val importances =
      List(Map(1L -> 1.0, 2L -> 1.0), Map(1L -> 1.0, 2L -> 0.5, 3L -> 6.0), Map(1L -> 1.0))
        .map(m => new Long2DoubleOpenHashMap(m.keys.toArray, m.values.toArray))
    val model =
      new RandomForestModel(
          importances
            .map(TestRegressionPredictorWithImportance(null, _, null).toMember)
            .toList, AveragingAggregatorFactory)
    val totalImportance = model.variableImportance
    assertEquals(Map(1L -> 1.0, 2L -> 0.5, 3L -> 2.0), totalImportance)
  }

  @Test
  def whenManyPredictorsThenAddsSplitCounts() {
    val splitCounts =
      List(Map(1L -> 1L, 2L -> 2L), Map(1L -> 1L, 2L -> 2L, 3L -> 6L), Map(1L -> 1L))
        .map(m => new Long2LongOpenHashMap(m.keys.toArray, m.values.toArray))
    val model =
      new RandomForestModel(
          splitCounts
            .map(TestRegressionPredictorWithImportance(null, null, _).toMember)
            .toList, AveragingAggregatorFactory)
    val totalSplitCount = model.variableSplitCount
    assertEquals(Map(1L -> 3L, 2L -> 4L, 3L -> 6L), totalSplitCount)
  }

  @Test
  def whenEmptyPredictsZero() {
    // AveragingAggregator with no trees → counts=0 → defaults to 0.0
    val model = new RandomForestModel(List(), AveragingAggregatorFactory)
    val prediction = model.predict(testData)
    assertArrayEquals(Array.fill(nSamples)(0.0), prediction.map(_.asInstanceOf[Double]),
      doubleComparisonDelta)
  }

  @Test
  def whenOnePredictorPassesThePrediction() {
    val assumedPredictions = Array(1.5, 3.0)
    val model =
      new RandomForestModel(List(TestRegressionPredictorWithImportance(assumedPredictions, null,
            null).toMember), AveragingAggregatorFactory)
    val prediction = model.predict(testData)
    assertArrayEquals(assumedPredictions, prediction.map(_.asInstanceOf[Double]),
      doubleComparisonDelta)
  }

  @Test
  def whenManyPredictorsThenPredictsByMean() {
    // Sample 0: (1.5 + 2.5 + 0.5) / 3 = 1.5
    // Sample 1: (3.0 + 1.0 + 2.0) / 3 = 2.0
    val assumedPredictions = List(Array(1.5, 3.0), Array(2.5, 1.0), Array(0.5, 2.0))
    val model =
      new RandomForestModel(
          assumedPredictions
            .map(TestRegressionPredictorWithImportance(_, null, null).toMember),
          AveragingAggregatorFactory)
    val prediction = model.predict(testData)
    assertArrayEquals(Array(1.5, 2.0), prediction.map(_.asInstanceOf[Double]),
      doubleComparisonDelta)
  }
}
