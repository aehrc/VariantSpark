package au.csiro.variantspark.algo

import org.apache.spark.mllib.linalg.Vectors
import org.junit.Assert.assertEquals
import org.junit.Test

class DecisionTreeNodeTest {

  @Test
  def testWhenLeafNodeTraverseReturnsIt() {
    val leafNode = LeafNode(7, 0, 0.0)
    assertEquals(leafNode, leafNode.traverse(_ => true))
  }

  @Test
  def testWhenSplitNodeTraversesToCorrectLeaf() {
    val leftLabel = 7
    val rightLabel = 9
    val splitNode = SplitNode(majorityLabel = 0, size = 10, nodeImpurity = 0.0, splitVariableIndex = 1L, splitPoint = 1.0,
      impurityReduction = 0.0, left = LeafNode(leftLabel, 0, 0.0), right = LeafNode(rightLabel, 0, 0.0))

    assertEquals(splitNode.left, splitNode.traverse(_ => true))
    assertEquals(splitNode.right, splitNode.traverse(_ => false))

  }
}
