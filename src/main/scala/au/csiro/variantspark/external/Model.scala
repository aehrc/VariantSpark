package au.csiro.variantspark.external

import au.csiro.variantspark.algo.RandomForestModel
import au.csiro.variantspark.algo.DecisionTreeNode
import au.csiro.variantspark.algo.LeafNode
import au.csiro.variantspark.algo.SplitNode
import au.csiro.variantspark.algo.RandomForestMember
import au.csiro.variantspark.algo.DecisionTreeModel

trait Node 

@SerialVersionUID(1l)
case class Leaf(val majorityLabel: Int, val  size: Int, val  impurity: Double) extends Node with Serializable

@SerialVersionUID(1l)
case class Split(val majorityLabel: Int, val size: Int, val  impurity: Double, 
    splitVar:String, splitVarIndex: Long, splitPoint: Double,
    impurityReduction: Double,left: Node, right: Node) extends Node with  Serializable

@SerialVersionUID(1l)
case class OOBInfo(val oobSamples:Array[Int], val oobPredictions:Array[Int]) extends Object with Serializable
    
@SerialVersionUID(1l)
case class Tree(val rootNode: Node, val oobInfo:Option[OOBInfo]) extends Object with Serializable

@SerialVersionUID(1l)
case class Forest(val trees:Seq[Tree]) extends Object with Serializable



