package au.csiro.variantspark

import org.apache.spark.mllib.linalg.Vector

package object algo {
  
  implicit val canSplitVector = new CanSplitVector
  implicit val canSplitArrayOfBytes = new CanSplitArrayByte
  
  type WideDecisionTree = DecisionTree[Vector]
  type WideDecisionTreeModel = DecisionTreeModel[Vector]
  
  type WideRandomForest = RandomForest[Vector]
  type WideRandomForestModel = RandomForestModel[Vector]
  
  type ByteRandomForestModel = RandomForestModel[Array[Byte]]
  type ByteRandomForest = RandomForest[Array[Byte]]
  
  
}