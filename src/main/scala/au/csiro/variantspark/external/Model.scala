package au.csiro.variantspark.external

import au.csiro.variantspark.algo.RandomForestModel
import au.csiro.variantspark.algo.DecisionTreeNode
import au.csiro.variantspark.algo.LeafNode
import au.csiro.variantspark.algo.SplitNode
import au.csiro.variantspark.algo.RandomForestMember
import au.csiro.variantspark.algo.DecisionTreeModel
import au.csiro.variantspark.algo.RandomForestParams

trait Node

@SerialVersionUID(1L)
case class Leaf(majorityLabel: Int, size: Int, impurity: Double) extends Node with Serializable

@SerialVersionUID(1L)
case class Split(majorityLabel: Int, size: Int, impurity: Double, splitVar: String,
    splitVarIndex: Long, permutated: Boolean, splitPoint: Double, impurityReduction: Double,
    left: Node, right: Node)
    extends Node with Serializable

@SerialVersionUID(1L)
case class OOBInfo(oobSamples: Array[Int], oobPredictions: Array[Int])
    extends Object with Serializable

@SerialVersionUID(1L)
case class Tree(rootNode: Node, oobInfo: Option[OOBInfo]) extends Object with Serializable

@SerialVersionUID(1L)
case class Forest(params: Option[RandomForestParams], trees: Seq[Tree],
    oobErrors: Option[Seq[Double]])
    extends Object with Serializable
