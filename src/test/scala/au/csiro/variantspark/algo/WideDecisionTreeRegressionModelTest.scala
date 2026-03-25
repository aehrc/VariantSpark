package au.csiro.variantspark.algo

import au.csiro.pbdava.ssparkle.common.utils.FastUtilConversions._
import au.csiro.variantspark.test.SparkTest
import org.apache.spark.mllib.linalg.Vectors
import org.junit.Assert._
import org.junit.Test
import au.csiro.variantspark.data.ContinuousVariable
import au.csiro.variantspark.data._
import au.csiro.variantspark.input._

class WideDecisionTreeRegressionModelTest extends SparkTest {

  @Test
  def testCorrectlyPredictsComplexTree() {
    // Tree with 2 split variables and 3 leaves; leaf prediction is the mean target value.
    // Feature 1L sample values: [0.0, 2.0, 2.0]  → sample 0 goes left, samples 1,2 go right
    // Feature 2L sample values: [0.0, 0.0, 1.0]  → of those, sample 1 goes left, sample 2 right
    val decisionTreeModel =
      new DecisionTreeModel(SplitNode(RegressionStats(0.0, 0, 0.0, 0.0), 1L,
          ThresholdSplitCriteria(1.0), 0.0,
          left = LeafNode(RegressionStats(0.0, 2, 3.0, 5.0)), // mean = 1.5
          right = SplitNode(RegressionStats(0.0, 0, 0.0, 0.0), 2L, ThresholdSplitCriteria(0.5),
            0.0, left = LeafNode(RegressionStats(0.0, 3, 6.0, 12.0)), // mean = 2.0
            right = LeafNode(RegressionStats(0.0, 4, 14.0, 49.0))))) // mean = 3.5
    val data = sc
      .parallelize(List(Vectors.dense(3.0, 3.0, 3.0), Vectors.dense(0.0, 2.0, 2.0),
          Vectors.dense(0.0, 0.0, 1.0)))
      .asFeature(ContinuousVariable)
    assertArrayEquals(Array(1.5, 2.0, 3.5),
      decisionTreeModel.predict(data).map(_.asInstanceOf[Double]), 1e-10)
  }

  @Test
  def testCorrectlyIdentifiedVariableImportanceForComplexTree() {
    // importance(var) = sum over split nodes on var of: node.size*node.impurity - left.size*left.impurity - right.size*right.impurity
    val decisionTreeModel =
      new DecisionTreeModel(SplitNode(RegressionStats(1.0, 10, 0.0, 0.0), 1L,
          ThresholdSplitCriteria(1.0), 0.0,
          left = SplitNode(RegressionStats(0.4, 4, 0.0, 0.0), 2L, ThresholdSplitCriteria(0.0), 0.0, left = LeafNode(RegressionStats(0.2, 3, 0.0, 0.0)), right = LeafNode(RegressionStats(0.1, 1, 0.0, 0.0))),
          right = SplitNode(RegressionStats(0.6, 6, 0.0, 0.0), 2L, ThresholdSplitCriteria(0.0), 0.0, left = LeafNode(RegressionStats(0.1, 2, 0.0, 0.0)), right = LeafNode(RegressionStats(0.2, 4, 0.0, 0.0)))))
    assertEquals(Map(1L -> (10 * 1.0 - (4 * 0.4 + 6 * 0.6)),
        2L -> ((4 * 0.4 - (3 * 0.2 + 1 * 0.1)) + (6 * 0.6 - (2 * 0.1 + 4 * 0.2)))),
      decisionTreeModel.variableImportanceAsFastMap.asScala)
  }

  @Test
  def testCorrectlyCountsSplitVariablesForComplexTree() {
    val decisionTreeModel =
      new DecisionTreeModel(SplitNode(RegressionStats(1.0, 10, 0.0, 0.0), 1L,
          ThresholdSplitCriteria(1.0), 0.0,
          left = SplitNode(RegressionStats(0.4, 4, 0.0, 0.0), 2L, ThresholdSplitCriteria(0.0), 0.0, left = LeafNode(RegressionStats(0.2, 3, 0.0, 0.0)), right = LeafNode(RegressionStats(0.1, 1, 0.0, 0.0))),
          right = SplitNode(RegressionStats(0.6, 6, 0.0, 0.0), 2L, ThresholdSplitCriteria(0.0), 0.0, left = LeafNode(RegressionStats(0.1, 2, 0.0, 0.0)), right = LeafNode(RegressionStats(0.2, 4, 0.0, 0.0)))))
    assertEquals(Map(1L -> 1L, 2L -> 2L), decisionTreeModel.variableSplitCountAsFastMap.asScala)
  }
}
