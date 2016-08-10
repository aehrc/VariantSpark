package au.csiro.variantspark.algo

import scala.Range
import scala.collection.JavaConversions.asScalaSet
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap
import scala.collection.mutable.MutableList
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.Logging
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap
import org.apache.spark.mllib.linalg.Vector
import au.csiro.pbdava.ssparkle.spark.SparkUtils
import au.csiro.variantspark.metrics.Gini

case class SubsetInfo(indices:Array[Int], impurity:Double, majorityLabel:Int) {
  def this(indices:Array[Int], impurity:Double, labels:Array[Int], nLabels:Int)  {
    this(indices, impurity, WideDecisionTree.labelMode(indices, labels, nLabels))
  }
  def lenght = indices.length
  override def toString(): String = s"SubsetInfo(${indices.toList},${impurity}, ${majorityLabel})"
}

case class SplitInfo(val splitPoint:Int, val gini:Double,  val leftGini:Double, val rightGini:Double) 

case class VarSplitInfo(val variableIndex: Long, val splitPoint:Int, val gini:Double, val leftGini:Double, val rightGini:Double) {
  
  def split(splitVarData:Map[Long, Vector], labels:Array[Int], nCategories:Int)(subset:SubsetInfo):List[SubsetInfo] = {
    List(
        new SubsetInfo(subset.indices.filter(splitVarData(variableIndex)(_) <= splitPoint), leftGini, labels, nCategories),
        new SubsetInfo(subset.indices.filter(splitVarData(variableIndex)(_) > splitPoint), rightGini, labels, nCategories)
    )
  }
}


/**
 * So what are the stopping conditions
 * - tree depth
 * - maximum number of nodes (which makes sense now becasue trees as build in the wide first manner)
 * - the minimum size of a node (only split nodes if they have more nodes than min)
 * - the node is pure (that is gini is 0) in which case there is not need to split
 * - the split does not improve gini
 * - the split will result in nodes that are too small???
 */


object WideDecisionTree {
  
  def labelMode(currentSet: Array[Int], labels: Array[Int], labelCount:Int): Int = {
    val labelCounts = Array.fill(labelCount)(0)
    currentSet.foreach(i => labelCounts(labels(i)) += 1)
    labelCounts.zipWithIndex.max._2
  }
  
  /**
   * This would take a variable information (index and data) 
   * labels and the indexed representation of the current splits 
   * and the split range to process
   * 
   * @param data  - the values of the variable for each sample
   * @param labels - the labels (categories) for each sample
   * @param splits - the indexed representation of the current splits
   * @param splitIndex - the split to consider
	*/
  
  def findSplit(data:Vector, labels:Array[Int], splitIndices:Array[Int]) = {
    //println("Split indexes: " + splitIndices.toList)
    // essentialy we need to find the best split for data and labels where splits[i] = splitIndex
    // would be nice perhaps if I couild easily subset my vectors
    
    // for now assume an ordered factor variable from [0, nFactorLen)
    // in which case (if we split with x <=n) we can split at {0,1, ..., nFactorLen-1} 
    // TODO: make an explicit parameter (also can subset by current split) --> essentially could be unique variable values
    val nFactorLen = data.toArray.max.toInt + 1
    // we need to exclude the last value as this give empty split    
    // TODO: filter out splits with not variability
    val splitCandidates = if (nFactorLen>1) Range(0, nFactorLen - 1) else Range(0,1)
        
    // but what if there is not variablity and all are zeroes?
    // println(splitCandidates)
    // we need to know totals per each labels (in order to be able to calculate total split gini)
    
    // TODO: make an explicit paramer
    val nCategories = labels.max + 1
    
    val totalLabelCounts = Array.fill(nCategories)(0)
    splitIndices.foreach { i => totalLabelCounts(labels(i)) += 1 }
    
    // not do the same for each split candidate and find the minimum one
    splitCandidates.map({splitPoint  => 
      val splitLabelCounts = Array.fill(nCategories)(0)
      splitIndices.foreach { i => if (data(i).toInt <= splitPoint) splitLabelCounts(labels(i)) += 1 }
      // TODO: here is where the gini calculation comes
      val (leftGini, rightGini, splitGini) = Gini.splitGiniInpurity(splitLabelCounts,totalLabelCounts) 
      SplitInfo(splitPoint, splitGini, leftGini, rightGini)
    }).reduce((t1,t2)=> if (t1.gini < t2.gini) t1 else t2)   
  }
  
  /**
   * This would take a variable information (index and data) 
   * labels and the indexed representation of the current splits 
   * and the split range to process
   * 
   * @param data  - the values of the variable for each sample
   * @param labels - the labels (categories) for each sample
   * @param splits - the indexed representation of the current splits
   * @param splitRange - the range of splits to consider
   * @param mTryFraction - the fraction of splits to apply this variable to
   */
  def findSplitsForVariableData(data:Vector, labels:Array[Int], splits:Array[Array[Int]], mTryFactor:Double) = {    
    splits.map(splitIndices =>  if (Math.random() <= mTryFactor) findSplit(data, labels, splitIndices) else null)
  }
  
  // but the whole thing is essentially an aggrefation on a map trying to find the best variable for each split 
  
  
  def merge(a1:Array[VarSplitInfo], a2:Array[VarSplitInfo]) = {
    def mergeSplitInfo(s1:VarSplitInfo, s2:VarSplitInfo) = {
      if (s1 == null) s2 else if (s2 == null) s1 else if (s1.gini < s2.gini) s1 else s2
    }
    Range(0,a1.length).foreach(i=> a1(i) = mergeSplitInfo(a1(i), a2(i)))
    a1
  }
  
  def findSplitsForVariable(br_labels:Broadcast[Array[Int]], br_splits:Broadcast[Array[Array[Int]]], mTryFactor:Double)(dataWithIndex:(Vector,Long))  = {
    val data = dataWithIndex._1
    val variableIndex = dataWithIndex._2
    val splitInfos = findSplitsForVariableData(data, br_labels.value, br_splits.value, mTryFactor)
    splitInfos.map(si => if (si != null) VarSplitInfo(variableIndex, si.splitPoint, si.gini, si.leftGini, si.rightGini) else null).toArray
  }
  
  def collectVariablesToMap(data: RDD[(Vector, Long)], variableIndexes:Set[Long]) =  {
      val br_variableIndexes = data.sparkContext.broadcast(variableIndexes)
      val result = data
        .filter({ case (data,variableIndex) => br_variableIndexes.value.contains(variableIndex)})
        .map(_.swap)
        .collectAsMap().toMap
      br_variableIndexes.destroy()
      result
  }
}

abstract class DecisionTreeNode(val majorityLabel: Int, val size: Int, val nodeImpurity: Double) {
  def isLeaf:Boolean
  def printout(level: Int) 
  def countImportance(accumulations: Long2DoubleOpenHashMap, totalSize:Int)
  def impurityContribution:Double = nodeImpurity * size
  def resolve(variables: Map[Long, Vector])(sampleIndex:Int):Int
}

case class LeafNode(override val majorityLabel: Int,override val  size: Int,override val  nodeImpurity: Double) extends DecisionTreeNode(majorityLabel,size,nodeImpurity) {
  val isLeaf = false

  def printout(level: Int) {
    print(new String(Array.fill(level)(' ')))
    val nodeType = "leaf"
    println(s"${nodeType}[${majorityLabel}, ${size}, ${nodeImpurity}]")
  }
  def countImportance(accumulations: Long2DoubleOpenHashMap, totalSize:Int) {}
  def resolve(variables: Map[Long, Vector])(sampleIndex:Int):Int = majorityLabel
}

case class SplitNode(override val majorityLabel: Int, override val size: Int,override val  nodeImpurity: Double, splitVariableIndex: Long, splitPoint: Double,
    impurityReduction: Double,left: DecisionTreeNode, right: DecisionTreeNode) extends DecisionTreeNode(majorityLabel,size,nodeImpurity) {
  
  val isLeaf = false
  
  def printout(level: Int) {
    print(new String(Array.fill(level)(' ')))
    val nodeType = "split"
    println(s"${nodeType}[${splitVariableIndex}, ${splitPoint}, ${majorityLabel}, ${size}, ${impurityReduction}, ${nodeImpurity}]")
    left.printout(level + 1)
    right.printout(level + 1)      
  }
  
  def countImportance(accumulations: Long2DoubleOpenHashMap, totalSize:Int) {
    accumulations.addTo(splitVariableIndex, (size*nodeImpurity - (left.impurityContribution + right.impurityContribution))/totalSize.toDouble)
    left.countImportance(accumulations, totalSize)
    right.countImportance(accumulations, totalSize)
  }

  def resolve(variables: Map[Long, Vector])(sampleIndex:Int):Int = if (variables(splitVariableIndex)(sampleIndex) <= splitPoint) left.resolve(variables)(sampleIndex) else right.resolve(variables)(sampleIndex)
}

class WideDecisionTreeModel(val rootNode: DecisionTreeNode) extends  Logging {

  def predict(data: RDD[Vector]): Array[Int] = predictIndexed(data.zipWithIndex())
  
  def predictIndexed(data: RDD[(Vector,Long)]): Array[Int] = {
    // this is a bit tricky but say lets' collect all the values neeed to resolve the thre

    //map the tree into a set of indexes

    def mapTrees(treeNode: DecisionTreeNode): List[Long] =  treeNode match {
      case SplitNode(_, _, _, variableIndex, _, _, left, right) => variableIndex :: mapTrees(left) ::: mapTrees(right)
      case _ => List()
    }
    val treeVariableData =  WideDecisionTree.collectVariablesToMap(data, mapTrees(rootNode).toSet)
    Range(0, data.first()._1.size).map(rootNode.resolve(treeVariableData)).toArray
  }

  def printout() {
    rootNode.printout(0)
  }

  def variableImportanceAsFastMap: Long2DoubleOpenHashMap = {
    val accumulations = new Long2DoubleOpenHashMap();
    rootNode.countImportance(accumulations, rootNode.size)
    accumulations
  }

  def variableImportance(): Map[Long, Double] = {
    variableImportanceAsFastMap.entrySet().map(e => (e.getKey.toLong, e.getValue.toDouble)).toMap
  }

}

case class DecisionTreeParams(val maxDepth:Int = 100, val minNodeSize:Int =1)

class WideDecisionTree(val params: DecisionTreeParams = DecisionTreeParams()) extends Logging {
  def run(data: RDD[Vector], labels: Array[Int]): WideDecisionTreeModel = run(data.zipWithIndex(), labels, Range(0, data.first().size).toArray, 0.3)
  def run(data: RDD[(Vector, Long)], labels: Array[Int], weights: Array[Int],nvarFraction: Double): WideDecisionTreeModel = {

    // TODO: not sure while this is need
    val dataSize = data.count()
    val currentSet =  Range(0,labels.length).filter(weights(_) > 0).toArray
    
    val br_labels = data.context.broadcast(labels)
    val br_weights = data.context.broadcast(weights)    
    
    val nCategories = labels.max + 1
    val (totalGini, totalLabel) = Gini.giniImpurity(currentSet, labels, nCategories)
    val rootNode = buildSplit(data, List(SubsetInfo(currentSet, totalGini, totalLabel)), br_labels,br_weights, nvarFraction, 0)
 
    br_labels.destroy()    
    new WideDecisionTreeModel(rootNode.head)
  }

  def buildSplit(indexedData: RDD[(Vector, Long)], subsets: List[SubsetInfo], br_labels: Broadcast[Array[Int]], br_weights: Broadcast[Array[Int]], nvarFraction: Double, treeLevel:Int): List[DecisionTreeNode] = {
      // for the current set find all candidate splits
  
      val nCategories = br_labels.value.max + 1
    
      logDebug(s"Building level ${treeLevel}, subsets: ${subsets}") 
       
      // pre-filter the subsets to check if they need further splitting
      // we also need to maintain the original index of the subset so that we can later 
      // join the next level tree nodes
      
      val subsetsToSplit = subsets.zipWithIndex.filter {case (si, _) =>
        si.lenght >= params.minNodeSize && treeLevel < params.maxDepth
      }
      logDebug(s"Splits to include: ${subsetsToSplit}") 
      
      //TODO: (OPTIMIZE) if there is not splits to calculate do not call compute splits etc.            
      //TODO: (OPTIMIZE) if I pass the subset info including impurity I can do post-filtering in workers
      
      val subsetsToSplitAsIndices = subsetsToSplit.map(_._1.indices).toArray
      val bestSplits = SparkUtils.withBrodcast(indexedData.sparkContext)(subsetsToSplitAsIndices){ br_splits => 
        indexedData
          .map(WideDecisionTree.findSplitsForVariable(br_labels,br_splits, nvarFraction))
          .fold(Array.fill(subsetsToSplitAsIndices.length)(null))(WideDecisionTree.merge)
      }
      logDebug(s"Best splits: ${bestSplits.toList}")

      // TODO: Wouild be good if all best splits were not null (if some are .. hmm this means probably sometthing is wrong with sampling
      
      // filter out splits that do not imporove impurity 
      val (usefulSplits, usefulSplitsIndices) = bestSplits.zip(subsetsToSplit)
            .map(t => ((t._1, t._2._1), t._2._2)) //  just remap from (x, (y, i)) => ((x,y),i)
            .filter({ case ((splitInfo,subsetInfo), i) => splitInfo != null && subsetInfo.impurity > splitInfo.gini})
            .unzip 
            
      // we need to collect the data for splitting variables so that we can calculate new subsets
      val usefulSplitsVarData = WideDecisionTree.collectVariablesToMap(indexedData, usefulSplits.map(_._1.variableIndex).toSet)
      
      // split current subsets into next level ones
      val nextLevelSubsets = usefulSplits.flatMap({ case (splitInfo, subset) => 
        splitInfo.split(usefulSplitsVarData, br_labels.value, nCategories)(subset)
      }).toList
     
     logDebug(s"Next level splits: ${nextLevelSubsets}")
     
     // compute the next level tree nodes (notice the recursive call)
     val nextLevelNodes = if (!nextLevelSubsets.isEmpty) buildSplit(indexedData, nextLevelSubsets, br_labels, br_weights, nvarFraction, treeLevel + 1) else List()

     // compute the indexes of splitted subsets against the original indexes
     val subsetIndexToSplitIndexMap = usefulSplitsIndices.zipWithIndex.toMap

     // I need to be able to find which nodes to use for splits
     // so I need say an Array that would tell me now which nodes were actually passed to split and what their index vas
     // at this stage we wouild know exactly what we need
     subsets.zipWithIndex.map({case (subset, i) => 
       subsetIndexToSplitIndexMap.get(i).map({splitIndex => 
         val split = usefulSplits(splitIndex)._1
         SplitNode(subset.majorityLabel, subset.lenght,  subset.impurity
             ,split.variableIndex, split.splitPoint, subset.impurity - split.gini,
             nextLevelNodes(2*splitIndex), nextLevelNodes(2*splitIndex+1))})
         .getOrElse(LeafNode(subset.majorityLabel, subset.lenght,  subset.impurity))
     }).toList
   }
}




