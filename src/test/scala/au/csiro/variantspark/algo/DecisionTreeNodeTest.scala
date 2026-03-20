package au.csiro.variantspark.algo

import org.junit.Assert.assertEquals
import org.junit.Test

class DecisionTreeNodeTest {

  @Test
  def testWhenLeafNodeTraverseReturnsIt() {
    val leafNode = LeafNode(ClassificationStats(0.0, 0, Array(1)))
    assertEquals(leafNode, leafNode.traverse(_ => true))
  }

  @Test
  def testWhenSplitNodeTraversesToCorrectLeaf() {
    val leftLeaf = LeafNode(ClassificationStats(0.0, 0, Array(0, 1)))
    val rightLeaf = LeafNode(ClassificationStats(0.0, 0, Array(0, 0, 0, 0, 0, 0, 0, 0, 0, 1)))
    val splitNode = SplitNode(
      ClassificationStats(0.0, 10, Array(1)),
      1L,
      ThresholdSplitCriteria(1.0),
      0.0,
      left = leftLeaf,
      right = rightLeaf)

    assertEquals(splitNode.left, splitNode.traverse(_ => true))
    assertEquals(splitNode.right, splitNode.traverse(_ => false))

  }
}
