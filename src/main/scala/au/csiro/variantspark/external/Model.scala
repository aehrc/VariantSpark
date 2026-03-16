package au.csiro.variantspark.external

import au.csiro.variantspark.algo.RandomForestModel
import au.csiro.variantspark.algo.DecisionTreeNode
import au.csiro.variantspark.algo.LeafNode
import au.csiro.variantspark.algo.SplitNode
import au.csiro.variantspark.algo.ImpurityStats
import au.csiro.variantspark.algo.SplitCriteria
import au.csiro.variantspark.algo.RandomForestMember
import au.csiro.variantspark.algo.DecisionTreeModel
import au.csiro.variantspark.algo.RandomForestParams

trait Node

@SerialVersionUID(1L)
case class Leaf(impurityStats: ImpurityStats) extends Node with Serializable

@SerialVersionUID(1L)
case class Split(impurityStats: ImpurityStats, splitVar: String, splitVarIndex: Long,
    permutated: Boolean, splitCriteria: SplitCriteria, impurityReduction: Double, left: Node,
    right: Node)
    extends Node with Serializable

@SerialVersionUID(1L)
case class OOBInfo(oobSamples: Array[Int], oobPredictions: Array[Any])
    extends Object with Serializable

@SerialVersionUID(1L)
case class Tree(rootNode: Node, oobInfo: Option[OOBInfo]) extends Object with Serializable

@SerialVersionUID(1L)
case class Forest(params: Option[RandomForestParams], trees: Seq[Tree],
    oobErrors: Option[Seq[Double]])
    extends Object with Serializable
