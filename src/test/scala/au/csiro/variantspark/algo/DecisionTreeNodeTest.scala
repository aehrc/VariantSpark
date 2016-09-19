package au.csiro.variantspark.algo

import org.apache.spark.mllib.linalg.Vectors
import org.junit.Assert.assertEquals
import org.junit.Test

class DecisionTreeNodeTest {

//  @Test
//  def testWhenLeafNodePredictsMajorityLabel() {
//    val testLabel = 7
//    val leafNode = LeafNode(7,0, 0.0)
//    assertEquals(testLabel, leafNode.predict(Map.empty)(0))
//  }
//
//  @Test
//  def testWhenSplitNodeNodePredictsCorrectSubnode() {
//    val leftLabel = 7
//    val rightLabel = 9
//    val splitNode = SplitNode(majorityLabel = 0, size = 10, nodeImpurity = 0.0,  splitVariableIndex = 1L,  splitPoint = 1.0, 
//        impurityReduction = 0.0, left = LeafNode(leftLabel,0, 0.0), right = LeafNode(rightLabel,0, 0.0))
//
//    val data = Map(1L -> Vectors.dense(0.0, 1.0, 2.0))     
//    assertEquals("Predicts left label for less then split point" , leftLabel, splitNode.predict(data)(0))
//    assertEquals("Predicts left label for equal to split point" , leftLabel, splitNode.predict(data)(1))
//    assertEquals("Predicts right label for greater than split point" , rightLabel, splitNode.predict(data)(2))
//  }  
}
