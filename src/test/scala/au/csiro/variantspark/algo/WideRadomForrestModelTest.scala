package au.csiro.variantspark.algo


import org.junit.Assert._
import org.junit.Test;
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import au.csiro.variantspark.test.SparkTest
import scala.collection.JavaConversions._

class WideRadomForrestModelTest extends SparkTest {
  val nLabels = 4
  val nSamples  = 2
  val testData = sc.parallelize(List(Vectors.zeros(nSamples)))
    
  @Test
  def whenEmptyPredictsHighestLabel() {
    val model = WideRandomForestModel(List(), nLabels)
    val prediction = model.predict(testData)
    assertArrayEquals(Array.fill(2)(0), prediction)
  }

  @Test
  def whenOnePredictorPassesThePrediction() {
    val assumedPreditions = Array(1,2)
    val model = WideRandomForestModel(List(TestPredictorWithImportance(assumedPreditions, null)), nLabels)
    val prediction = model.predict(testData)
    assertArrayEquals(assumedPreditions, prediction)
  }

  @Test
  def whenManyPreditorsThenPredictsByVoting() {
    val assumedPreditions = List(Array(1,0), Array(1,2), Array(1,0))
    val model = WideRandomForestModel(assumedPreditions.map(TestPredictorWithImportance(_, null)).toList, nLabels)
    val prediction = model.predict(testData)
    assertArrayEquals(Array(1,0), prediction)
  }

  
  def whenManyPredictorsThenAveragesImportnace() {
    val importances = List(Map(1l -> 1.0, 2l->1.0), Map(1l->1.0, 2l->0.5, 3l->0.3), Map(1L-> 1.0)).map(m => new Long2DoubleOpenHashMap(m.keys.toArray, m.values.toArray))
    val model = WideRandomForestModel(importances.map(TestPredictorWithImportance(null,_)).toList, nLabels)
    val totalImportnace = model.variableImportance
    assertEquals(Map(1L->1.0, 2L->0.5, 3L->0.1), totalImportnace)
  }
  
}