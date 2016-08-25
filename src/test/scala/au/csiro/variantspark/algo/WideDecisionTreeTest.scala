package au.csiro.variantspark.algo

import org.junit.Assert._
import org.junit.Test;
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import au.csiro.variantspark.test.SparkTest
import au.csiro.pbdava.ssparkle.common.utils.FastUtilConversions._

class WideDecisionTreeTest {
  
  
  @Test
  def testSplitsCorrectlyForFullData() {
    val data = Vectors.dense(1.0, 2.0, 1.0, 2.0)
    val labels = Array(0,1,0,1)
    val split = WideDecisionTree.findSplit(data, labels, Range(0, data.size).toArray)
    assertEquals(SplitInfo(1, 0.0, 0.0, 0.0), split)
  }

  @Test
  def testSplitsCorrectlyForIndexdData() {
    val data = Vectors.dense(1.0, 2.0, 1.0, 2.0, 1.0, 1.0)
    val labels = Array(0,1,0,1,1,0)
    val split = WideDecisionTree.findSplit(data, labels, Range(0, data.size - 2).toArray)
    assertEquals(SplitInfo(1, 0.0, 0.0, 0.0), split)
  }

}