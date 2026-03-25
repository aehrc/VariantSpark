package au.csiro.variantspark.algo

import org.junit.Assert.assertEquals
import org.junit.Test

class DecisionTreeRegressionNodeTest {

  @Test
  def testWhenLeafNodeTraverseReturnsIt() {
    val leafNode = LeafNode(RegressionStats(0.0, 1, 2.5, 6.25))
    assertEquals(leafNode, leafNode.traverse(_ => true))
  }

  @Test
  def testWhenSplitNodeTraversesToCorrectLeaf() {
    val leftLeaf = LeafNode(RegressionStats(0.0, 3, 6.0, 12.0))
    val rightLeaf = LeafNode(RegressionStats(0.25, 4, 10.0, 27.0))
    val splitNode = SplitNode(RegressionStats(0.5, 7, 16.0, 39.0), 1L,
      ThresholdSplitCriteria(1.0), 0.0, left = leftLeaf, right = rightLeaf)

    assertEquals(splitNode.left, splitNode.traverse(_ => true))
    assertEquals(splitNode.right, splitNode.traverse(_ => false))
  }
}
