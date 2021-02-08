package au.csiro.variantspark.algo

import au.csiro.variantspark.data._
import au.csiro.variantspark.test.SparkTest
import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.junit.Assert._
import org.junit.Test

class WideRandomForrestModelTest extends SparkTest {
  val doubleComparisonDelta = 1e-6
  val nLabels = 4
  val nSamples = 2
  val testData: RDD[(Feature, Long)] =
    sc.parallelize(List(Vectors.zeros(nSamples))).asFeature(BoundedOrdinalVariable(3))

  @Test
  def whenManyPredictorsThenAveragesImportance() {
    val importances =
      List(Map(1L -> 1.0, 2L -> 1.0), Map(1L -> 1.0, 2L -> 0.5, 3L -> 6.0), Map(1L -> 1.0))
        .map(m => new Long2DoubleOpenHashMap(m.keys.toArray, m.values.toArray))
    val model =
      new RandomForestModel(importances.map(TestPredictorWithImportance(null, _).toMember).toList,
        nLabels)
    val totalImportance = model.variableImportance
    assertEquals(Map(1L -> 1.0, 2L -> 0.5, 3L -> 2.0), totalImportance)
  }

  @Test
  def whenEmptyPredictsHighestLabel() {
    val model = new RandomForestModel(List(), nLabels)
    val prediction = model.predict(testData)
    assertArrayEquals(Array.fill(2)(0), prediction)
  }

  @Test
  def whenOnePredictorPassesThePrediction() {
    val assumedPredictions = Array(1, 2)
    val model =
      new RandomForestModel(List(TestPredictorWithImportance(assumedPredictions, null).toMember),
        nLabels)
    val prediction = model.predict(testData)
    assertArrayEquals(assumedPredictions, prediction)
  }

  @Test
  def whenManyPredictorsThenPredictsByVoting() {
    val assumedPredictions = List(Array(1, 0), Array(1, 2), Array(1, 0))
    val model =
      new RandomForestModel(
          assumedPredictions
            .map(TestPredictorWithImportance(_, null).toMember), nLabels)
    val prediction = model.predict(testData)
    assertArrayEquals(Array(1, 0), prediction)
  }

  @Test
  def predictProbabilities() {
    val assumedPredictions = List(Array(1, 1), Array(1, 2), Array(1, 1), Array(1, 1))
    val model =
      new RandomForestModel(
          assumedPredictions
            .map(TestPredictorWithImportance(_, null).toMember), nLabels)
    val classProbabilities = model.predictProb(testData)
    assertArrayEquals(Array(0.0, 1.0, 0.0, 0.0), classProbabilities(0), doubleComparisonDelta)
    assertArrayEquals(Array(0.0, 0.75, 0.25, 0.0), classProbabilities(1), doubleComparisonDelta)
  }

}
