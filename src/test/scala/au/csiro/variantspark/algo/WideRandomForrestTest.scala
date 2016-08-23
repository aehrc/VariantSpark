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
import au.csiro.variantspark.utils.Sample
import scala.collection.mutable.MutableList

class WideRandomForrestTest extends SparkTest {
  val nSamples  = 100
  val nLabels = nSamples
  val testData = sc.parallelize(List(Vectors.zeros(nSamples))).zipWithIndex()
  val labels = Array.fill(nLabels)(1)

  @Test
  def testBuildsCorrectBoostedModelWithoutOob() {
    val nTryFraction = 0.6
    val collector = new TreeDataCollector()
    val rf = new WideRandomForest(RandomForestParams(oob=false, nTryFraction = nTryFraction, bootstrap=true), modelBuilder = collector.collectData)
    val model = rf.train(testData, labels, 10)
    assertEquals("All trees in the model", collector.allTreest, model.trees)
    assertTrue("All trees trained on the same data", collector.allData.forall(_ == testData))
    assertTrue("All trees trained with expected nTryFactor", collector.allnTryFration.forall(_ == nTryFraction))
    assertTrue("All trees trained same labels", collector.allLabels.forall(_ == labels))
    assertTrue("All trees trained with requested samples", collector.allSamples.forall(s => s.nSize == nSamples && !s.indexesOut.isEmpty))    
  }

  @Test
  def testBuildsCorrectUnBoostedModelWithOob() {
    val nTryFraction = 0.6
    val nTrees = 10
    val collector = new TreeDataCollector(Stream.continually(1).map(pl => TestPredictorWithImportance(Array.fill(nLabels)(pl), null)))   
    val rf = new WideRandomForest(RandomForestParams(oob=true, nTryFraction = nTryFraction, bootstrap=false, subsample = 0.5), modelBuilder = collector.collectData)
    val model = rf.train(testData, labels, nTrees)
    assertEquals("All trees in the model", collector.allTreest, model.trees)
    assertTrue("All trees trained on the same data", collector.allData.forall(_ == testData))
    assertTrue("All trees trained with expected nTryFactor", collector.allnTryFration.forall(_ == nTryFraction))
    assertTrue("All trees trained same labels", collector.allLabels.forall(_ == labels))
    // the oob errors should follow the 1 0 1 pattern
    // as even trees predict all 0 and odd trees all 1
    assertEquals("Oob erros should always decrease", model.oobErrors.sortBy(-_), model.oobErrors)
    assertEquals("The first error should be 0.5", 0.5, model.oobErrors.head, 0)
    assertEquals("The last error should be 0", 0, model.oobErrors.last, 0)
    assertTrue("All trees trained with requested samples", collector.allSamples.forall(s => s.lenght == nSamples/2 && !s.indexesOut.isEmpty))    
  }

}