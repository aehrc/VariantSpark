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
import au.csiro.pbdava.ssparkle.spark.SparkUtils._
import au.csiro.variantspark.metrics.Gini
import au.csiro.variantspark.utils.Sample
import au.csiro.pbdava.ssparkle.common.utils.FastUtilConversions._
import au.csiro.variantspark.utils.VectorRDDFunction._
import au.csiro.variantspark.utils.FactorVariable
import org.apache.commons.math3.random.RandomGenerator
import org.apache.commons.math3.random.JDKRandomGenerator
import au.csiro.pbdava.ssparkle.common.utils.Prof
import it.unimi.dsi.util.XorShift1024StarRandomGenerator
import au.csiro.variantspark.data.VariableType
import au.csiro.variantspark.data.BoundedOrdinal
import au.csiro.variantspark.utils.defRng

case class SubsetInfo(indices:Array[Int], impurity:Double, majorityLabel:Int) {
  def this(indices:Array[Int], impurity:Double, labels:Array[Int], nLabels:Int)  {
    this(indices, impurity, FactorVariable.labelMode(indices, labels, nLabels))
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

  def split(data:Vector, labels:Array[Int], nCategories:Int)(subset:SubsetInfo):(SubsetInfo, SubsetInfo) = {
    (
        new SubsetInfo(subset.indices.filter(data(_) <= splitPoint), leftGini, labels, nCategories),
        new SubsetInfo(subset.indices.filter(data(_) > splitPoint), rightGini, labels, nCategories)
    )
  }
}


object VarSplitInfo {
  def apply(variableIndex:Long, split:SplitInfo):VarSplitInfo  = apply(variableIndex, split.splitPoint, split.gini, split.leftGini, split.rightGini)
}

case class VariableSplitter(val dataType: VariableType, val labels:Array[Int], mTryFactor:Double=1.0) extends Logging  with Prof {
  
  val nCategories = labels.max + 1
  
  def findSplits(data:Vector, splits:Array[SubsetInfo])(implicit rng:RandomGenerator):Array[SplitInfo] = {
    val splitter = dataType match {
      case BoundedOrdinal(nLevels) => new JConfusionClassificationSplitter(labels, nCategories, nLevels)
      case _ => throw new RuntimeException(s"Data type ${dataType} not supported")
    }
    // now I can filter out splits that do not improve gini
    splits.map { subsetInfo =>  
      if (rng.nextDouble() <= mTryFactor) { 
        val splitInfo = splitter.findSplit(data.toArray, subsetInfo.indices)
        if (splitInfo != null && splitInfo.gini < subsetInfo.impurity) splitInfo else null
      } else null 
    }
  }
  
  def findSplitsForVars(varData:Iterator[(Vector, Long)], splits:Array[SubsetInfo])(implicit rng:RandomGenerator):Iterator[Array[VarSplitInfo]] = {
    profIt("Local: splitting") {
      val result = varData
        .map(vi => findSplits(vi._1, splits).map(si => if (si != null) VarSplitInfo(vi._2, si) else null).toArray)
        .foldLeft(Array.fill[VarSplitInfo](splits.length)(null))(WideDecisionTree.merge)  
      // the fold above can be done by spark 
      // but this help to optimize for performance
      Some(result).toIterator
    }
  }
  
  def splitSubsets(varData:Iterator[(Vector, Long)], subsets:Array[SubsetInfo], bestSplits:Array[VarSplitInfo])  = {
      val usefulSubsetSplitAndIndex = subsets.zip(bestSplits).filter(_._2 != null).zipWithIndex.toList
      val splitByVarIndex = usefulSubsetSplitAndIndex.groupBy(_._1._2.variableIndex)
      varData.flatMap { case (v,i) => 
        splitByVarIndex.getOrElse(i, Nil).map { case ((subsetInfo, splitInfo), si) =>
            (si, splitInfo.split(v, labels, nCategories)(subsetInfo))
        }
     }    
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


object WideDecisionTree extends Logging  with Prof {
  
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
  // but the whole thing is essentially an aggrefation on a map trying to find the best variable for each of the splits
  // in the list (they represents best splits a tree level)
  
  def merge(a1:Array[VarSplitInfo], a2:Array[VarSplitInfo]) = {
    def mergeSplitInfo(s1:VarSplitInfo, s2:VarSplitInfo) = {
      if (s1 == null) s2 else if (s2 == null) s1 else if (s1.gini < s2.gini) s1 else if (s2.gini < s1.gini) s2 else if (s1.variableIndex < s2.variableIndex) s1 else s2
    }
    Range(0,a1.length).foreach(i=> a1(i) = mergeSplitInfo(a1(i), a2(i)))
    a1
  }
  
  def findSplitsForVariable(br_splits:Broadcast[Array[SubsetInfo]], br_splitter:Broadcast[VariableSplitter])
      (varData:Iterator[(Vector, Long)])  =  br_splitter.value.findSplitsForVars(varData, br_splits.value)(new XorShift1024StarRandomGenerator())
  
      
  def splitSubsets(indexedData: RDD[(Vector, Long)], bestSplits:Array[VarSplitInfo], br_subsets:Broadcast[Array[SubsetInfo]], br_splitter:Broadcast[VariableSplitter]) = {
    // TODO: Perhaps do not need to broadcast empty values
    profIt("REM: splitSubsets") { 
      val indexedSplittedSubsets = withBroadcast(indexedData)(bestSplits){ br_bestSplits =>       
        // this becomes really complex will try to simplify it later 
        indexedData.mapPartitions(it => br_splitter.value.splitSubsets(it, br_subsets.value, br_bestSplits.value))
          .collectAsMap()      
      }
      // for subsets into  an array
      indexedSplittedSubsets.foldLeft(Array.fill[SubsetInfo](indexedSplittedSubsets.size*2)(null)) { case (a, (i, st)) =>
        a(2*i) = st._1
        a(2*i+1) = st._2
        a
      }.toList
    }
  }
      
  def findBestSplits(indexedData: RDD[(Vector, Long)], br_splits:Broadcast[Array[SubsetInfo]], br_splitter:Broadcast[VariableSplitter])
        (implicit rng:RandomGenerator) =  {
    val seed = rng.nextLong()
    profIt("REM: findBestSplits") { 
      indexedData
        .mapPartitionsWithIndex { case (pi, it) =>
          br_splitter.value.findSplitsForVars(it, br_splits.value)(new XorShift1024StarRandomGenerator(seed ^ pi))
        }.fold(Array.fill(br_splits.value.length)(null))(WideDecisionTree.merge)  
    }
  }
}

abstract class DecisionTreeNode(val majorityLabel: Int, val size: Int, val nodeImpurity: Double) {
  def isLeaf:Boolean
  
  def printout(level: Int) 
  def impurityContribution:Double = nodeImpurity * size
  def predict(variables: Map[Long, Vector])(sampleIndex:Int):Int
  def toStream:Stream[DecisionTreeNode]
  def splitsToStream:Stream[SplitNode]  = toStream.filter(!_.isLeaf).asInstanceOf[Stream[SplitNode]]
  def leafsToStream:Stream[LeafNode]  = toStream.filter(_.isLeaf).asInstanceOf[Stream[LeafNode]]
}

case class LeafNode(override val majorityLabel: Int,override val  size: Int,override val  nodeImpurity: Double) extends DecisionTreeNode(majorityLabel,size,nodeImpurity) {  
  val isLeaf = true

  def printout(level: Int) {
    print(new String(Array.fill(level)(' ')))
    val nodeType = "leaf"
    println(s"${nodeType}[${majorityLabel}, ${size}, ${nodeImpurity}]")
  }
  def predict(variables: Map[Long, Vector])(sampleIndex:Int):Int = majorityLabel
  def toStream:Stream[DecisionTreeNode] = this #:: Stream.empty
}

object LeafNode {
  def apply(subset:SubsetInfo):LeafNode = apply(subset.majorityLabel, subset.lenght, subset.impurity)
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
  
  def childFor(value:Double):DecisionTreeNode =  if (value <= splitPoint) left else right
  
  // TODO (CHECK): Not sure but this can be the same as impurity reduction
  def impurityDelta  = impurityContribution - (left.impurityContribution + right.impurityContribution)
  def predict(variables: Map[Long, Vector])(sampleIndex:Int):Int = if (variables(splitVariableIndex)(sampleIndex) <= splitPoint) left.predict(variables)(sampleIndex) else right.predict(variables)(sampleIndex)
  def toStream:Stream[DecisionTreeNode] = this #:: left.toStream #::: right.toStream
}

object SplitNode {
  def apply(subset:SubsetInfo, split:VarSplitInfo, left: DecisionTreeNode, right: DecisionTreeNode): SplitNode = apply(subset.majorityLabel, subset.lenght,  subset.impurity
                 ,split.variableIndex, split.splitPoint, subset.impurity - split.gini, left, right)
}

class WideDecisionTreeModel(val rootNode: DecisionTreeNode) extends PredictiveModelWithImportance with  Logging {
  
  def splitVariableIndexes = rootNode.splitsToStream.map(_.splitVariableIndex).toSet
  
  def predictIndexed(indexedData: RDD[(Vector,Long)]): Array[Int] = {
    val treeVariableData =  indexedData.collectAtIndexes(splitVariableIndexes)
    Range(0, indexedData.size).map(rootNode.predict(treeVariableData)).toArray
  }

  def printout() {
    rootNode.printout(0)
  }

  def variableImportanceAsFastMap: Long2DoubleOpenHashMap = rootNode.splitsToStream.
    foldLeft(new Long2DoubleOpenHashMap()){ case (m, splitNode) => 
      m.increment(splitNode.splitVariableIndex, splitNode.impurityDelta)
    }
  
  def impurity = rootNode.toStream.map(_.nodeImpurity).toList
  def variables = rootNode.splitsToStream.map(_.splitVariableIndex).toList
  def threshods = rootNode.splitsToStream.map(_.splitPoint).toList
}


object WideDecisionTreeModel {
  
  def resolveSplitNodes(indexedData: RDD[(Vector,Long)], splitNodes:List[(SplitNode, Int)]) = {
    val varsAndIndexesToCollect = splitNodes.asInstanceOf[List[(SplitNode, Int)]].map { case (n,i) => (n.splitVariableIndex, i)}.zipWithIndex.toArray
      // broadcast
      val varValuesForSplits = withBroadcast(indexedData)(varsAndIndexesToCollect) { br_varsAndIndexesToCollect =>
        indexedData.mapPartitions{ it => 
          // group by variable index
          val varsAndIndexesToCollectMap = br_varsAndIndexesToCollect.value.toList.groupBy(_._1._1)
          it.flatMap { case (v,vi) =>
            varsAndIndexesToCollectMap.getOrElse(vi, Nil).map { case (n,si) => (si, v(n._2)) }
          }
      }.collectAsMap
    }
    splitNodes.asInstanceOf[List[(SplitNode, Int)]].zipWithIndex.map{ case ((n,i),v) => (n.childFor(varValuesForSplits(v)), i)}  
  }
  
  //
  // predict same sample for all trees
  // as the result though I would like to get predictions for each tree not just one so an int[nTrees][nIndexes]
  def batchPredict(indexedData: RDD[(Vector,Long)], trees:Seq[WideDecisionTreeModel], indexes:Seq[Array[Int]]) = {
    // samples are the same so can be broacasted
    // maybe I can broadcast trees as well ...
    // but in general i should iterate through levels and for each prediction point send out the variable that should be retrieved (and say -1 if none)
    // then I wil get the entire vector I can decide the next point
    
    // use recursion to replace loop
    def predict(nodesAndIndexes:List[((DecisionTreeNode, Int), Int)]):List[((LeafNode, Int), Int)] = {
      val (leaves, splits) = nodesAndIndexes.partition(_._1._1.isLeaf)
      if (splits.isEmpty) {
        leaves.asInstanceOf[List[((LeafNode, Int), Int)]]
      } else {        
        val (bareSplits, splitIndexes) = splits.unzip
        val transfomedSplits = resolveSplitNodes(indexedData, bareSplits.asInstanceOf[List[(SplitNode, Int)]]).zip(splitIndexes)
        leaves.asInstanceOf[List[((LeafNode, Int), Int)]] ::: predict(transfomedSplits)
      }
    }
    
    val rootNodesAndIndexes = trees.map(_.rootNode).zip(indexes).flatMap { case (n,idx) => idx.map(i => (n,i)) }.zipWithIndex.toList
    val leaveNodesAndIndexes = predict(rootNodesAndIndexes)
    // somehow need to get to the original prediction
    
    val orderedPredictions = leaveNodesAndIndexes.sortBy(_._2).unzip._1.map(_._1.majorityLabel)
    val orderedPredictionsIter = orderedPredictions.toIterator
    
    // how to merge it back now?
    // well should be in the same order as original ??? ( NO - the order got corrupted in paritioning process)))
    // so will need to preserve it and sort by or do something else but say it's presrved
    //then I just need to fill the values with an iterator
    
    //hmm not to sure if map is sequential 
    indexes.map(a => Array.fill(a.length)(orderedPredictionsIter.next()))
  }
}

case class DecisionTreeParams(
    val maxDepth:Int = Int.MaxValue, 
    val minNodeSize:Int = 1,
    val seed:Long = defRng.nextLong )

class WideDecisionTree(val params: DecisionTreeParams = DecisionTreeParams()) extends Logging with Prof {
  
  // TODO (Design): This seems like an easiest solution but it make this class 
  // to keep random state ... perhaps this could be externalised to the implicit random
  implicit lazy val rnd = new XorShift1024StarRandomGenerator(params.seed)
  
  def run(data: RDD[Vector], dataType: VariableType, labels: Array[Int]): WideDecisionTreeModel = run(data.zipWithIndex(), dataType, labels, 1.0, Sample.all(data.first().size))
  
  def run(indexedData: RDD[(Vector, Long)], dataType: VariableType,  labels: Array[Int], nvarFraction: Double, sample:Sample): WideDecisionTreeModel =
      batchTrain(indexedData, dataType, labels, nvarFraction, List(sample)).head

  /**
   * Trains all the trees for specified samples at the same time
   */
  def batchTrain(indexedData: RDD[(Vector, Long)], dataType: VariableType, 
      labels: Array[Int], nvarFraction: Double, sample:Seq[Sample]): Seq[WideDecisionTreeModel] = {
    //TODO (OPTIMIZE): Perhpas is't better to use unique indexes and weights
    val splitter = VariableSplitter(dataType, labels, nvarFraction)
    val subsets = sample.map { s => 
      val currentSet =  s.indexesIn.toArray
      val (totalGini, totalLabel) = Gini.giniImpurity(currentSet, labels, splitter.nCategories)
      SubsetInfo(currentSet, totalGini, totalLabel)
    }.toList
    val rootNodes = withBroadcast(indexedData)(splitter) { br_splitter => 
      buildSplit(indexedData, subsets, br_splitter, 0)
    }
    rootNodes.map(new WideDecisionTreeModel(_))
  }
  
  /**
   * Builds (recursively) the decision tree level by level
   */
  
  def buildSplit(indexedData: RDD[(Vector, Long)], subsets: List[SubsetInfo], br_splitter: Broadcast[VariableSplitter],treeLevel:Int): List[DecisionTreeNode] = {

    // TODO (Note): This still seem to be somewhat obsucure do the the indexing used perhaps now couild be done in a single pass
    // but then I do need to aggregate splitts for subcalls - so pehraps some indexin is unavoidable
    // unless I do filtering on server side ????
    
    // for the current set find all candidate splits    
    logDebug(s"Building level ${treeLevel}, subsets: ${subsets}")       
    profReset()
    // pre-filter the subsets to check if they need further splitting
    // we also need to maintain the original index of the subset so that we can later 
    // join the next level tree nodes
    
    val subsetsToSplit = subsets.zipWithIndex.filter {case (si, _) =>
      si.lenght >= params.minNodeSize && treeLevel < params.maxDepth
    }
    logDebug(s"Splits to include: ${subsetsToSplit}") 
    
    //TODO: (OPTIMIZE) if there is not splits to calculate do not call compute splits etc.            
      
    val (bestSplits, nextLevelSubsets) = findBestSplitsAndSubsets(indexedData, subsetsToSplit.unzip._1.toList, br_splitter)
    logDebug(s"Best splits: ${bestSplits.toList}")
    logDebug(s"Next level splits: ${nextLevelSubsets}")
    
    profPoint("Best splits and splitting done")
 
    // compute the next level tree nodes (notice the recursive call)
    val nextLevelNodes = if (!nextLevelSubsets.isEmpty) buildSplit(indexedData, nextLevelSubsets, br_splitter, treeLevel + 1) else List()
     
    profPoint("Sublevesl done")
    
    val (usefulSplits, usefulSplitsIndices) = bestSplits.zip(subsetsToSplit.unzip._2).filter(_._1 != null).unzip
    // compute the indexes of splitted subsets against the original indexes
    val subsetIndexToSplitIndexMap = usefulSplitsIndices.zipWithIndex.toMap

    // TODO (Optimize): I use oplain arrays that obscure things somewhat assuming they are faster to serialise, but perhpas the differnece is negligable and 
    // I could use for example an array of tuples rather the obscure indexig scheme

    
    // I need to be able to find which nodes to use for splits
    // so I need say an Array that would tell me now which nodes were actually passed to split and what their index vas
    // at this stage we wouild know exactly what we need
     val result = subsets.zipWithIndex.map {case (subset, i) => 
       subsetIndexToSplitIndexMap.get(i)
         .map(splitIndex => SplitNode(subset, usefulSplits(splitIndex),nextLevelNodes(2*splitIndex), nextLevelNodes(2*splitIndex+1)))
         .getOrElse(LeafNode(subset))
     }.toList
     profPoint("building done")
     
    result 
  }
  
  def findBestSplitsAndSubsets(indexedData: RDD[(Vector, Long)], subsetsToSplit:List[SubsetInfo], br_splitter:Broadcast[VariableSplitter]) =  {
    profIt("findBestSplitsAndSubsets") { 
      val subsetsToSplitAsIndices = subsetsToSplit.toArray
      withBroadcast(indexedData)(subsetsToSplitAsIndices){ br_splits => 
        val bestSplits = WideDecisionTree.findBestSplits(indexedData, br_splits, br_splitter)          
        // now with the same broadcasted splits
        (bestSplits, WideDecisionTree.splitSubsets(indexedData, bestSplits, br_splits, br_splitter))
      }
    }
  }
}




