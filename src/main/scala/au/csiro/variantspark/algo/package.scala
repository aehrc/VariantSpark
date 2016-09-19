package au.csiro.variantspark

import org.apache.spark.mllib.linalg.Vector

package object algo {
  
  implicit val canSplitVector = new CanSplitVector
  
  type WideDecisionTree = DecisionTree[Vector]
  type WideDecisionTreeModel = DecisionTreeModel[Vector]
}