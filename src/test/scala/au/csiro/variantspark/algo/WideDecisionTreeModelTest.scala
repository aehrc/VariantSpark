package au.csiro.variantspark.algo

import au.csiro.pbdava.ssparkle.common.utils.FastUtilConversions._
import au.csiro.variantspark.test.SparkTest
import org.apache.spark.mllib.linalg.Vectors
import org.junit.Assert._
import org.junit.Test
import au.csiro.variantspark.data.ContinuousVariable
import au.csiro.variantspark.data._
import au.csiro.variantspark.input._

class WideDecisionTreeModelTest extends SparkTest {

  @Test
  def testCorrectlyPredictsComplexTree() {
    // let's build a tree with 2 variables and 5 nodes
    val decisionTreeModel = new DecisionTreeModel(SplitNode.voting(majorityLabel = 0, size = 10,
        nodeImpurity = 0.0, splitVariableIndex = 1L, splitPoint = 1.0, impurityReduction = 0.0,
        left = LeafNode.voting(1, 0, 0.0),
        right = SplitNode.voting(majorityLabel = 0, size = 10, nodeImpurity = 0.0, splitVariableIndex = 2L, splitPoint = 0.0, impurityReduction = 0.0, left = LeafNode.voting(2, 0, 0.0), right = LeafNode.voting(3, 0, 0.0))))
    val data = sc
      .parallelize(List(Vectors.dense(3.0, 3.0, 3.0), Vectors.dense(0.0, 2.0, 2.0),
          Vectors.dense(0.0, 0.0, 1.0)))
      .asFeature(ContinuousVariable)
    assertArrayEquals(Array(1, 2, 3), decisionTreeModel.predict(data))
  }

  @Test
  def testCorrectlyIdentifiedVariableImportanceForComplexTree() {
    // let's build a tree with 2 variables and 5 nodes
    val decisionTreeModel = new DecisionTreeModel(SplitNode.voting(majorityLabel = 0, size = 10,
        nodeImpurity = 1.0, splitVariableIndex = 1L, splitPoint = 1.0, impurityReduction = 0.0,
        left = SplitNode.voting(majorityLabel = 0, size = 4, nodeImpurity = 0.4, splitVariableIndex = 2L, splitPoint = 0.0, impurityReduction = 0.0, left = LeafNode.voting(2, 3, 0.2), right = LeafNode.voting(3, 1, 0.1)),
        right = SplitNode.voting(majorityLabel = 0, size = 6, nodeImpurity = 0.6, splitVariableIndex = 2L, splitPoint = 0.0, impurityReduction = 0.0, left = LeafNode.voting(2, 2, 0.1), right = LeafNode.voting(3, 4, 0.2))))
    assertEquals(Map(1L -> (10 * 1.0 - (4 * 0.4 + 6 * 0.6)),
        2L -> ((4 * 0.4 - (3 * 0.2 + 1 * 0.1)) + (6 * 0.6 - (2 * 0.1 + 4 * 0.2)))),
      decisionTreeModel.variableImportanceAsFastMap.asScala)
  }
}
