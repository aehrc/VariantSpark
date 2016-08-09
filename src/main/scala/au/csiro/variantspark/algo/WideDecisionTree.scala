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


case class SplitInfo(val splitPoint:Int, val gini:Double) 

case class VarSplitInfo(val variableIndex: Long, val splitPoint:Int, val gini:Double) 


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
  
  def sqr(x: Double) = x * x

  def giniImpurityWithTotal(counts: Array[Int]): (Double, Int) = {
    val total = counts.sum
    val totalAsDouble = total.toDouble
    if (total == 0) (0.0, total) else (1 - counts.map(s => sqr(s / totalAsDouble)).sum, total)
  }

  
  def giniImpurity(counts: Array[Int]): Double = giniImpurityWithTotal(counts)._1

  
  def splitGiniInpurity(leftCounts: Array[Int], totalCounts:Array[Int]) =  {
    val (leftGini, leftTotal) = giniImpurityWithTotal(leftCounts)
    val (rightGini, rightTotal) = giniImpurityWithTotal(totalCounts.zip(leftCounts).map(t => t._1 -t._2).toArray)
    (leftGini, rightGini, (leftGini* leftTotal + rightGini* rightTotal)/(leftTotal + rightTotal))
  }
  
  def giniImpurity(currentSet: Array[Int], labels: Array[Int], labelCount:Int):(Double,Int) = {
    val labelCounts = Array.fill(labelCount)(0)
    currentSet.foreach(i => labelCounts(labels(i)) += 1)
    (giniImpurity(labelCounts), labelCounts.zipWithIndex.max._2)
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
    
    // essentialy we need to find the best split for data and labels where splits[i] = splitIndex
    // would be nice perhaps if I couild easily subset my vectors
    
    // for now assume an ordered factor variable from [0, nFactorLen)
    // in which case (if we split with x <=n) we can split at {0,1, ..., nFactorLen-1} 
    // TODO: make an explicit parameter (also can subset by current split) --> essentially could be unique variable values
    val nFactorLen = data.toArray.max.toInt + 1
    val splitCandidates = Range(0, nFactorLen)
    
    // we need to know totals per each labels (in order to be able to calculate total split gini)
    
    // TODO: make an explicit paramer
    val nCategories = labels.max + 1
    
    val totalLabelCounts = Array.fill(0)(0)
    splitIndices.foreach { i => totalLabelCounts(labels(i)) += 1 }
    
    // not do the same for each split candidate and find the minimum one
    splitCandidates.map({splitPoint  => 
      val splitLabelCounts = Array.fill(0)(0)
      splitIndices.foreach { i => if (data(i).toInt <= splitPoint) splitLabelCounts(labels(i)) += 1 }
      // TODO: here is where the gini calculation comes
      val (leftGini, rightGini, splitGini) = splitGiniInpurity(splitLabelCounts,totalLabelCounts) 
      SplitInfo(splitPoint, splitGini)
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
  
  def findSplitsForVariable(br_labels:Broadcast[Array[Int]], splits:Array[Array[Int]], mTryFactor:Double)(dataWithIndex:(Vector,Long)) = {
    val data = dataWithIndex._1
    val variableIndex = dataWithIndex._2
    val splitInfos = findSplitsForVariableData(data, br_labels.value, splits, mTryFactor)
    splitInfos.map(si => if (si != null) VarSplitInfo(variableIndex, si.splitPoint, si.gini) else null).toArray
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

  def resolve(variables: Map[Long, Vector])(sampleIndex:Int):Int = if (variables(splitVariableIndex)(sampleIndex) <= splitPoint) left.resolve(variables)(sampleIndex) else left.resolve(variables)(sampleIndex)
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

case class SubsetInfo(indices:Array[Int], impurity:Double) {
  def lenght = indices.length
}

case class DecisionTreeParams(val maxDepth:Int = 5, val minNodeSize:Int =0)

class WideDecisionTree(val params: DecisionTreeParams = DecisionTreeParams()) {
  def run(data: RDD[Vector], labels: Array[Int]): WideDecisionTreeModel = run(data.zipWithIndex(), labels, Range(0, data.first().size).toArray, 0.3)
  def run(data: RDD[(Vector, Long)], labels: Array[Int], weights: Array[Int],nvarFraction: Double): WideDecisionTreeModel = {

    // TODO: not sure while this is need
    val dataSize = data.count()
    val currentSet =  weights.map(c => if (c>0) 0 else -1).toArray
    
    val br_labels = data.context.broadcast(labels)
    val br_weights = data.context.broadcast(weights)    
    val rootNode = buildSplit(data, List(SubsetInfo(Range(0,labels.length).toArray, 1.0)), br_labels,br_weights, nvarFraction, 0)
 
    br_labels.destroy()    
    new WideDecisionTreeModel(rootNode.head)
  }

  def buildSplit(indexedData: RDD[(Vector, Long)], splitInfos: List[SubsetInfo], br_labels: Broadcast[Array[Int]], br_weights: Broadcast[Array[Int]], nvarFraction: Double, treeLevel:Int): List[DecisionTreeNode] = {
      // for the current set find all candidate splits
  
      // say for not the criteria are  minNodeSize && notPure && there is giniGain
      
      // this is where we can actually do the filtering to see which nodes need further splitting
      // only split these that needs splitting otherwise just create a leave node for others.
      // some filtering may also be needed after the calculation to identify nodes with not gini gain
      
      
      val  indexedSplitsToInclude  = splitInfos.
        map(si => si.lenght >= params.minNodeSize && treeLevel < params.maxDepth)
        .foldLeft((List[Option[Int]](),0))({ case ((l,c),includeSplit) => if (includeSplit) ( Some(c) :: l, c + 1)  else (None :: l, c) })
        ._1.reverse
        
      val splitsToInclude = indexedSplitsToInclude.zip(splitInfos).filter(_._1.isDefined).map(_._2)
      val splits = splitsToInclude.map(_.indices).toArray
   
      val bestSplits = indexedData
        .map(WideDecisionTree.findSplitsForVariable(br_labels,splits, nvarFraction))
        .fold(Array.fill(splitInfos.length)(null))(WideDecisionTree.merge)
      // TODO: Wouild be good if all best splits were not null (if some are .. hmm this means probably sometthing is wrong with sampling
      
      // TODO: somewhere here we need to remove splits that do not have any gini gain (or that are null)
        
        
      
      // well actually what we need to do is to update the split set by creating new splits for all not empty splits
      // so not we wouild have to collect all the data need to calculate the actual splits
      
      val bestVariablesData = WideDecisionTree.collectVariablesToMap(indexedData, bestSplits.map(_.variableIndex).toSet)
      
      // generate new split set
      // filter out splits with no gini gain
      val nextLevelSplits = splitsToInclude.zip(bestSplits).flatMap({ case(splitInfo, bestSplit) =>
        // assuming it's not null
        List(SubsetInfo(splitInfo.indices.filter(bestVariablesData(bestSplit.variableIndex)(_) <= bestSplit.splitPoint), 0.0)
            ,SubsetInfo(splitInfo.indices.filter(bestVariablesData(bestSplit.variableIndex)(_) > bestSplit.splitPoint), 0.0))
      }).toList
     
     
     val nextLevelNodes = buildSplit(indexedData, nextLevelSplits, br_labels, br_weights, nvarFraction, treeLevel + 1)
     // I need to be able to find which nodes to use for splits
     // so I need say an Array that would tell me now which nodes were actually passed to split and what their index vas
     // at this stage we wouild know exactly what we need
     splitInfos.zipWithIndex.map({case (splitInfo, i) => 
       indexedSplitsToInclude(i).map(ni => SplitNode(0, splitInfo.lenght,  splitInfo.impurity , 0L, 1, 1.0,  nextLevelNodes(2*ni), nextLevelNodes(2*ni+1))).getOrElse(LeafNode(0, splitInfo.lenght,  splitInfo.impurity))
     }).toList
   }
}




