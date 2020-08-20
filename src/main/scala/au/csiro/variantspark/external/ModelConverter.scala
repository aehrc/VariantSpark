package au.csiro.variantspark.external

import au.csiro.variantspark.algo.RandomForestModel
import au.csiro.variantspark.algo.RandomForestMember
import au.csiro.variantspark.algo.DecisionTreeModel
import au.csiro.variantspark.algo.DecisionTreeNode
import au.csiro.variantspark.algo.LeafNode
import au.csiro.variantspark.algo.SplitNode
import org.apache.spark.rdd.RDD
import au.csiro.pbdava.ssparkle.spark.SparkUtils

class ModelConverter(varIndex: Map[Long, String]) {

  def toExternal(node: DecisionTreeNode): Node = {
    node match {
      case LeafNode(majorityLabel, classCounts, size, nodeImpurity) =>
        Leaf(majorityLabel, classCounts, size, nodeImpurity)
      case SplitNode(majorityLabel, classCounts, size, nodeImpurity, splitVariableIndex,
          splitPoint, impurityReduction, left, right, isPermutated) => {
        Split(majorityLabel, classCounts, size, nodeImpurity,
          varIndex.getOrElse(splitVariableIndex, null), splitVariableIndex, isPermutated,
          splitPoint, impurityReduction, toExternal(left), toExternal(right))
      }
      case _ => throw new IllegalArgumentException("Unknow node type:" + node)
    }
  }

  def toExternal(rfMember: RandomForestMember): Tree = {
    val rootNode = rfMember.predictor match {
      case DecisionTreeModel(rootNode) => toExternal(rootNode)
      case _ => throw new IllegalArgumentException("Unknow predictory type:" + rfMember.predictor)
    }
    Tree(rootNode,
      Option(rfMember.oobIndexes).map(_ => OOBInfo(rfMember.oobIndexes, rfMember.oobPred)))
  }

  def toExternal(rfModel: RandomForestModel): Forest = {
    val oobErrors =
      if (rfModel.oobErrors != null && rfModel.oobErrors.nonEmpty
          && !rfModel.oobErrors.head.isNaN) {
        Some(rfModel.oobErrors)
      } else {
        None
      }
    Forest(Option(rfModel.params), rfModel.members.map(toExternal), oobErrors)
  }
}
