package au.csiro.variantspark.algo

import au.csiro.pbdava.ssparkle.common.utils.FastUtilConversions._
import au.csiro.variantspark.test.SparkTest
import org.apache.spark.mllib.linalg.Vectors
import org.junit.Assert._
import org.junit.Test


class WideDecisionTreeModelTest extends SparkTest {

  @Test
  def testCorrectlyPredictsComplexTree() {
    // let's build a tree with 2 variables and 5 nodes
    val decisionTreeModel = new WideDecisionTreeModel(
      SplitNode(majorityLabel = 0, size = 10, nodeImpurity = 0.0, splitVariableIndex = 1L, splitPoint = 1.0, impurityReduction = 0.0,
        left = LeafNode(1, 0, 0.0),
        right = SplitNode(majorityLabel = 0, size = 10, nodeImpurity = 0.0, splitVariableIndex = 2L, splitPoint = 0.0, impurityReduction = 0.0,
          left = LeafNode(2, 0, 0.0),
          right = LeafNode(3, 0, 0.0)
        )
      )
    )
    val data = sc.parallelize(List(
      Vectors.dense(3.0, 3.0, 3.0),
      Vectors.dense(0.0, 2.0, 2.0),
      Vectors.dense(0.0, 0.0, 1.0)
    )).zipWithIndex
    assertArrayEquals(Array(1, 2, 3), decisionTreeModel.predictIndexed(data))
  }

  @Test
  def testCorrectlyIdentifiedVariableImportanceForComplexTree() {
    // let's build a tree with 2 variables and 5 nodes
    val decisionTreeModel = new WideDecisionTreeModel(
      SplitNode(majorityLabel = 0, size = 10, nodeImpurity = 1.0, splitVariableIndex = 1L, splitPoint = 1.0, impurityReduction = 0.0,
        left = SplitNode(majorityLabel = 0, size = 4, nodeImpurity = 0.4, splitVariableIndex = 2L, splitPoint = 0.0, impurityReduction = 0.0,
          left = LeafNode(2, 3, 0.2),
          right = LeafNode(3, 1, 0.1)
        ),
        right = SplitNode(majorityLabel = 0, size = 6, nodeImpurity = 0.6, splitVariableIndex = 2L, splitPoint = 0.0, impurityReduction = 0.0,
          left = LeafNode(2, 2, 0.1),
          right = LeafNode(3, 4, 0.2)
        )
      )
    )
    assertEquals(Map(1L -> (10 * 1.0 - (4 * 0.4 + 6 * 0.6)), 2L -> ((4 * 0.4 - (3 * 0.2 + 1 * 0.1)) + (6 * 0.6 - (2 * 0.1 + 4 * 0.2)))), decisionTreeModel.variableImportanceAsFastMap.asScala)
  }
}

