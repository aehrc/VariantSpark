package au.csiro.variantspark.algo

import au.csiro.pbdava.ssparkle.common.utils.FastUtilConversions._
import au.csiro.variantspark.test.SparkTest
import org.apache.spark.mllib.linalg.Vectors
import org.junit.Assert._
import org.junit.Test
import au.csiro.variantspark.data.ContinuousVariable
import au.csiro.variantspark.data._
import au.csiro.variantspark.input._

class WideDecisionTreeClassificationModelTest extends SparkTest {

  @Test
  def testCorrectlyPredictsComplexTree() {
    // let's build a tree with 2 variables and 5 nodes
    val decisionTreeModel =
      new DecisionTreeModel(SplitNode(ClassificationStats(0.0, 10, Array(1)), 1L,
          ThresholdSplitCriteria(1.0), 0.0,
          left = LeafNode(ClassificationStats(0.0, 0, Array(0, 1))),
          right = SplitNode(ClassificationStats(0.0, 10, Array(1)), 2L, ThresholdSplitCriteria(0.0), 0.0, left = LeafNode(ClassificationStats(0.0, 0, Array(0, 0, 1))), right = LeafNode(ClassificationStats(0.0, 0, Array(0, 0, 0, 1))))))
    val data = sc
      .parallelize(List(Vectors.dense(3.0, 3.0, 3.0), Vectors.dense(0.0, 2.0, 2.0),
          Vectors.dense(0.0, 0.0, 1.0)))
      .asFeature(ContinuousVariable)
    assertArrayEquals(Array(1, 2, 3), decisionTreeModel.predict(data).map(_.asInstanceOf[Int]))
  }

  @Test
  def testCorrectlyIdentifiedVariableImportanceForComplexTree() {
    // let's build a tree with 2 variables and 5 nodes
    val decisionTreeModel =
      new DecisionTreeModel(SplitNode(ClassificationStats(1.0, 10, Array(1)), 1L,
          ThresholdSplitCriteria(1.0), 0.0,
          left = SplitNode(ClassificationStats(0.4, 4, Array(1)), 2L, ThresholdSplitCriteria(0.0), 0.0, left = LeafNode(ClassificationStats(0.2, 3, Array(0, 0, 1))), right = LeafNode(ClassificationStats(0.1, 1, Array(0, 0, 0, 1)))),
          right = SplitNode(ClassificationStats(0.6, 6, Array(1)), 2L, ThresholdSplitCriteria(0.0), 0.0, left = LeafNode(ClassificationStats(0.1, 2, Array(0, 0, 1))), right = LeafNode(ClassificationStats(0.2, 4, Array(0, 0, 0, 1))))))
    assertEquals(Map(1L -> (10 * 1.0 - (4 * 0.4 + 6 * 0.6)),
        2L -> ((4 * 0.4 - (3 * 0.2 + 1 * 0.1)) + (6 * 0.6 - (2 * 0.1 + 4 * 0.2)))),
      decisionTreeModel.variableImportanceAsFastMap.asScala)
  }
  @Test
  def testCorrectlyCountsSplitVariablesForComplexTree() {
    // let's build a tree with 2 variables and 5 nodes
    val decisionTreeModel =
      new DecisionTreeModel(SplitNode(ClassificationStats(1.0, 10, Array(1)), 1L,
          ThresholdSplitCriteria(1.0), 0.0,
          left = SplitNode(ClassificationStats(0.4, 4, Array(1)), 2L, ThresholdSplitCriteria(0.0), 0.0, left = LeafNode(ClassificationStats(0.2, 3, Array(0, 0, 1))), right = LeafNode(ClassificationStats(0.1, 1, Array(0, 0, 0, 1)))),
          right = SplitNode(ClassificationStats(0.6, 6, Array(1)), 2L, ThresholdSplitCriteria(0.0), 0.0, left = LeafNode(ClassificationStats(0.1, 2, Array(0, 0, 1))), right = LeafNode(ClassificationStats(0.2, 4, Array(0, 0, 0, 1))))))
    assertEquals(Map(1L -> 1L, 2L -> 2L), decisionTreeModel.variableSplitCountAsFastMap.asScala)
  }
}
