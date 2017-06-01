package au.csiro.variantspark.algo

import scala.Range
import scala.reflect.ClassTag
import scala.collection.JavaConversions.asScalaSet
import scala.collection.mutable.MutableList

import au.csiro.pbdava.ssparkle.spark.SparkUtils._
import au.csiro.pbdava.ssparkle.common.utils.FastUtilConversions._
import au.csiro.pbdava.ssparkle.common.utils.Logging
import au.csiro.pbdava.ssparkle.common.utils.Prof

import au.csiro.variantspark.ata.BoundedOrdinal
import au.csiro.variantspark.data.VariableTyped
import au.csiro.variantspark.metrics.Gini
import au.csiro.variantspark.utils.{defRng, CanSize, FactorVariable, IndexedRDDFunction._, Sample}

import org.apache.commons.lang3.builder.ToStringBuilder
import org.apache.commons.math3.random.{RandomGenerator, JDKRandomGenerator}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.{RDD, RDD.rddToPairRDDFunctions}
import org.apache.spark.TaskContext

import it.unimi.dsi.fastutil.{longs.Long2DoubleOpenHashMap, ints.Int2ObjectOpenHashMap}
import it.unimi.dsi.util.XorShift1024StarRandomGenerator

/** Splits whatever is passed at the index specified
  */
trait CanSplit[V] extends CanSize[V] {

  /** Returns the SplitInfo class located below
    *
    * Specify the 'v', 'splitter', 'indicies', and 'SplitInfo'
    *
    * @param v: generic type, input specific data construct
    * @param splitter: input a classificationsplitter object, splits the values in v according to it's properties
    * @param indicies: input an array of indicies of type int
    * @return SplitInfo: returns info on the splits conducted touch on in [[au.csiro.variantspark.algo.SplitInfo]]
    */
  def split(v:V, splitter: ClassificationSplitter, indices:Array[Int]):SplitInfo

  /** Returns value at a certain index
    * 
    * Specify the 'v' and 'i'
    *
    *
    * @param v: generic type, input specific data construct
    * @param i: index
    */
  def at(v:V)(i:Int):Int
}

/** Allows for a general description of the construct 
  *
  * Specify the 'indices', 'impurtity', and 'majoritylabel' these values will not be visible outside the class
  *
  * {{{
  * val subInfo = SubsetInfo(indices, impurtity, majorityLabel)
  * val subInfoAlt = SubsetInfo(indices, impurity, labels, nLabels)
  * }}}
  *
  * @constructor creates value based on the indices, impurity, and majorityLabel
  * @constructor this: an alterative constructor for the use of the FactorVariable class
  * @param indices: input an array of integers representing the indices of the values
  * @param impurity: input the value of impurity of the data construct
  * @param majorityLabel: input the specific label related to the majority of the values
  */
case class SubsetInfo(indices:Array[Int], impurity:Double, majorityLabel:Int) {

  /** An alternative constructor for the SubsetInfo class, use this if the majorityLabel has not already been defined
    * 
    * Specify the 'indices', 'impurity', 'labels', and 'nLabels'
    *
    * {{{
    * val subInfo = SubsetInfo(indices, impurtity, labels, nLables)
    * }}}
    *
    * @param indices: input an array of integers that contains the indicies required
    * @param inpurity: a value based on the gini impurity of the dataset sent in
    * @param labels: in put an array of integers that contains the labels of the values for each row
    * @param nLabels: specify the number of labels for that specific dataset
    *
    */
  def this(indices:Array[Int], impurity:Double, labels:Array[Int], nLabels:Int)  {
    this(indices, impurity, FactorVariable.labelMode(indices, labels, nLabels))
  }

  def length = indices.length
  override def toString(): String = s"SubsetInfo(${indices.toList},${impurity}, ${majorityLabel})"
}

/** An immutable container for the information that was recently split 
  * 
  * Specify 'splitPoint', 'gini', 'leftGini', and 'rightGini'
  *
  * @constructor create an object containing the information about the split
  * @param splitPoint: specifies the exact point in the dataset that it was split at
  * @param gini: general gini value of the dataset
  * @param leftGini: the gini impurity of the left split of the dataset
  * @param rightGini: the gini impurity of the right split of the dataset
  */
case class SplitInfo(val splitPoint:Int, val gini:Double,  val leftGini:Double, val rightGini:Double)

/** Class utilized to give an insight into the split data
  * 
  * Specify the 'variableIndex', 'splitPoint', 'gini', 'leftGini', and 'rightGini'
  *
  * @constructor creates information about the split that occured on a specifc variable
  * @param variableIndex: specifies the index of the variable that the dataset will split on
  * @param splitPoint: specifies the point in the index of the exact split
  * @param gini: general gini value of the dataset
  * @param leftGini: the gini impurity of the left split of the dataset
  * @param rightGini: the gini impurity of the right split of the dataset 
  */
case class VarSplitInfo(val variableIndex: Long, val splitPoint:Int, val gini:Double, val leftGini:Double, val rightGini:Double) {

  /** Creates a list of the subsetInfos for the dataset split
    *
    * @param splitVarData: a map of long key values and a generic type construct for the dataset
    * @param labels: input an array of integer labels
    * @param nCategories: specify the number of categories of the dataset or 'columns'
    * @param subset: specify the SubsetInfo class touched on previously at [[au.csiro.variantspark.algo.SubsetInfo]]
    * @param canSplit: from the CanSplit trait will be specified automatically touch on previous at [[au.csiro.variantspark.algo.CanSplit]]
    * @return returns a list containing information about the subsets created in the split, refer to the SubsetInfo class
    */
  def split[V](splitVarData:Map[Long, V], labels:Array[Int], nCategories:Int)(subset:SubsetInfo)(implicit canSplit:CanSplit[V]):List[SubsetInfo] = {
    List(
        new SubsetInfo(subset.indices.filter(canSplit.at(splitVarData(variableIndex))(_) <= splitPoint), leftGini, labels, nCategories),
        new SubsetInfo(subset.indices.filter(canSplit.at(splitVarData(variableIndex))(_) > splitPoint), rightGini, labels, nCategories)
    )
  }

  /** Creates a list of the subsetInfos for the dataset split
    *
    * @param data: input the specific data construct
    * @param labels: input an array of integer labels
    * @param nCategories: specify the number of categories of the dataset or 'columns'
    * @param subset: specify the SubsetInfo class touched on previously at [[algo.SubsetInfo]]
    * @param canSplit: from the CanSplit trait will be specified automatically touch on previous at [[algo.CanSplit]]
    * @return returns a tupple of the subset information
    */
  def split[V](data:V, labels:Array[Int], nCategories:Int)(subset:SubsetInfo)(implicit canSplit:CanSplit[V]):(SubsetInfo, SubsetInfo) = {
    (
        new SubsetInfo(subset.indices.filter(canSplit.at(data)(_) <= splitPoint), leftGini, labels, nCategories),
        new SubsetInfo(subset.indices.filter(canSplit.at(data)(_) > splitPoint), rightGini, labels, nCategories)
    )
  }
}

/** Utilized to return a VarSplitInfo object
  */
object VarSplitInfo {

  /** Applys the values obtained from the [[au.csiro.variantspark.algo.SplitInfo]] class to create an [[au.csiro.variantspark.algo.VarSplitInfo]] object
    *
    * @param variableIndex: input an index of where the variable split from  
    * @param split: input a [[au.csiro.variantspark.algo.SplitInfo]] object
    * @return returns a [[au.csiro.variantspark.algo.VarSplitInfo]] object
    */
  def apply(variableIndex:Long, split:SplitInfo):VarSplitInfo  = apply(variableIndex, split.splitPoint, split.gini, split.leftGini, split.rightGini)
}

/** Defines the trait for the case class [[au.csiro.variantspark.algo.DeterministicMerger]]
  */
trait Merger {

  /** Operates a merging function utilizing two arrays of the class [[au.csiro.variantspark.algo.VarSplitInfo]]
    *
    * @param a1: input an array of [[au.csiro.variantspark.algo.VarSplitInfo]] 
    * @param a2: input an array of [[au.csiro.variantspark.algo.VarSplitInfo]]
    * @return Returns an array of [[au.csiro.variantspark.algo.VarSplitInfo]]
    */
  def merge(a1:Array[VarSplitInfo], a2:Array[VarSplitInfo]):  Array[VarSplitInfo]
}

/** Utilizes the Deterministic Decision Tree model found here: [[https://en.wikipedia.org/wiki/Decision_tree_model#Randomized_decision_tree]]
  * Extends the [[au.csiro.variantspark.algo.Merger]] class
  */
case class DeterministicMerger() extends Merger {

  /** Operates a merging function utilizing two arrays of the class [[au.csiro.variantspark.algo.VarSplitInfo]]
    *
    * @param a1: input an array of [[au.csiro.variantspark.algo.VarSplitInfo]] 
    * @param a2: input an array of [[au.csiro.variantspark.algo.VarSplitInfo]]
    * @return Returns the merged array a1
    */
  def merge(a1:Array[VarSplitInfo], a2:Array[VarSplitInfo]) = {

    // TODO: this seems to introduce bias towards low index variables which may make them appear
    // more important than they actually are
    // to avoid that (in case of gini equality) select a random variable

    /** Takes the [[au.csiro.variantspark.algo.VarSplitInfo]] from two seperate splits and returns the value from either s1 or s2 based on the gini impurity
      *
      * @param s1: input an [[au.csiro.variantspark.algo.VarSplitInfo]] 
      * @param s2: input an [[au.csiro.variantspark.algo.VarSplitInfo]]
      * @return Returns either s1 or s2 based on the gini impurity calculation
      */
    def mergeSplitInfo(s1:VarSplitInfo, s2:VarSplitInfo) = {
      if (s1 == null) s2 else if (s2 == null) s1 else if (s1.gini < s2.gini) s1 else if (s2.gini < s1.gini) s2 else if (s1.variableIndex < s2.variableIndex) s1 else s2
    }
    Range(0,a1.length).foreach(i=> a1(i) = mergeSplitInfo(a1(i), a2(i)))
    a1
  }
}

/** Utilizes the Randomized Decision Tree model found here: [[https://en.wikipedia.org/wiki/Decision_tree_model#Randomized_decision_tree]]
  * Extends the [[au.csiro.variantspark.algo.Merger]] class
  *
  * @param seed: input a seed value to initialize the random number generator for rnd
  */
case class RandomizingMerger(seed:Long) extends Merger {

  lazy val rnd  = new  XorShift1024StarRandomGenerator(seed ^ TaskContext.getPartitionId())

  /** Operates a merging function utilizing two arrays of the class [[au.csiro.variantspark.algo.VarSplitInfo]]
    *
    * @param a1: input an array of [[au.csiro.variantspark.algo.VarSplitInfo]] 
    * @param a2: input an array of [[au.csiro.variantspark.algo.VarSplitInfo]]
    * @return Returns the merged array a1
    */
  def merge(a1:Array[VarSplitInfo], a2:Array[VarSplitInfo]) = {

    // TODO: this seems to introduce bias towards low index variables which may make them appear
    // more important than they actually are
    // in oder to avoid that (in case of gini equality) select a random variable

    /** Takes the [[au.csiro.variantspark.algo.VarSplitInfo]] from two seperate splits and returns the value from either s1 or s2 based on the gini impurity
      *
      * @note if the gini values of each split are equal then the value returns one at random
      *
      * @param s1: input an [[au.csiro.variantspark.algo.VarSplitInfo]] 
      * @param s2: input an [[au.csiro.variantspark.algo.VarSplitInfo]]
      * @return Returns either s1 or s2 based on the gini impurity calculation
      */
    def mergeSplitInfo(s1:VarSplitInfo, s2:VarSplitInfo) = {
      if (s1 == null) s2 else if (s2 == null) s1 else if (s1.gini < s2.gini) s1 else if (s2.gini < s1.gini) s2 else if (rnd.nextBoolean()) s1 else s2
    }
    Range(0,a1.length).foreach(i=> a1(i) = mergeSplitInfo(a1(i), a2(i)))
    a1
  }
}

/** This is the main split function
  * 
  * 1. specifies the number of categories based on the label input
  * 2. 
  *
  * @param dataType: specify the basic data type of the variable split on
  * @param labels: input an array of labels used by the dataset
  * @param mTryFactor: default to 1.0
  * @param randomizedEquality: default to false
  */
case class VariableSplitter[V](val dataType: VariableType, val labels:Array[Int], mTryFactor:Double=1.0, val randomizeEquality:Boolean = false)(implicit canSplit:CanSplit[V]) extends Logging  with Prof {

  val nCategories = labels.max + 1

  /** Find the splits in the data based on the gini value 
    * 
    * Specify the 'data' and 'splits' inputs
    *
    * @param data: input the data from the dataset of generic type V
    * @param splits: input an array of the [[au.csiro.variantspark.algo.SubsetInfo]] class
    * @return returns an array [[au.csiro.variantspark.algo.SplitInfo]]
    */
  def findSplits(data:V, splits:Array[SubsetInfo])(implicit rng:RandomGenerator):Array[SplitInfo] = {

    /** if the dataType of the 
      */
    val splitter = dataType match {
      case BoundedOrdinal(nLevels) => new JConfusionClassificationSplitter(labels, nCategories, nLevels)
      case _ => throw new RuntimeException(s"Data type ${dataType} not supported")
    }
    // TODO: Filter out splits that do not improve gini

    splits.map { subsetInfo =>
      if (rng.nextDouble() <= mTryFactor) {

        // TODO (CanSplit): use a better method

        val splitInfo = canSplit.split(data, splitter, subsetInfo.indices)
        if (splitInfo != null && splitInfo.gini < subsetInfo.impurity) splitInfo else null
      } else null
    }
  }

  def findSplitsForVars(varData:Iterator[(V, Long)], splits:Array[SubsetInfo])(implicit rng:RandomGenerator):Iterator[Array[VarSplitInfo]] = {
    profIt("Local: splitting") {
      val result = varData
        .map{vi =>
          val thisVarSplits = findSplits(vi._1, splits)
          thisVarSplits.map(si => if (si != null) VarSplitInfo(vi._2, si) else null).toArray
        }
      // TODO: .foldLeft(Array.fill[VarSplitInfo](splits.length)(null))(DecisionTree.merge)
      // the fold above can be done by spark
      // and the line below will help to optimize performance
      // Some(result).toIterator
      result
    }
  }

  def splitSubsets(varData:Iterator[(V, Long)], subsets:Array[SubsetInfo], bestSplits:Array[VarSplitInfo])  = {
      val usefulSubsetSplitAndIndex = subsets.zip(bestSplits).filter(_._2 != null).zipWithIndex.toList
      val splitByVarIndex = usefulSubsetSplitAndIndex.groupBy(_._1._2.variableIndex)
      varData.flatMap { case (v,i) =>
        splitByVarIndex.getOrElse(i, Nil).map { case ((subsetInfo, splitInfo), si) =>
            (si, splitInfo.split(v, labels, nCategories)(subsetInfo))
        }
     }
  }

  def createMerger(seed:Long):Merger = if (randomizeEquality) RandomizingMerger(seed) else DeterministicMerger()

}


/**
 * So what are the stopping conditions
 * - tree depth
 * - maximum number of nodes (which makes sense now because trees as build in the wide first manner)
 * - the minimum size of a node (only split nodes if they have more nodes than min)
 * - the node is pure (that is gini is 0) in which case there is not need to split
 * - the split does not improve gini
 * - the split will result in nodes that are too small???
 */

object DecisionTree extends Logging  with Prof {

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
  //noinspection ScalaDocUnknownParameter,ScalaDocUnknownParameter,ScalaDocUnknownParameter,ScalaDocUnknownParameter,ScalaDocUnknownParameter
  // but the whole thing is essentially an aggregation on a map trying to find the best variable for each of the splits
  // in the list (they represents best splits a tree level)

//  def merge(a1:Array[VarSplitInfo], a2:Array[VarSplitInfo]) = {
//    // TODO: this seem to introduce bias towards low index variables which may make them appear
//    // more important than they actually are
//    // in oder to avoid that in case of gini equaliy a random variable should be selected
//    def mergeSplitInfo(s1:VarSplitInfo, s2:VarSplitInfo) = {
//      if (s1 == null) s2 else if (s2 == null) s1 else if (s1.gini < s2.gini) s1 else if (s2.gini < s1.gini) s2 else if (s1.variableIndex < s2.variableIndex) s1 else s2
//    }
//    Range(0,a1.length).foreach(i=> a1(i) = mergeSplitInfo(a1(i), a2(i)))
//    a1
//  }

//  def findSplitsForVariable[V](br_splits:Broadcast[Array[SubsetInfo]], br_splitter:Broadcast[VariableSplitter[V]])
//      (varData:Iterator[(V, Long)])  =  br_splitter.value.findSplitsForVars(varData, br_splits.value)(new XorShift1024StarRandomGenerator())


  def splitSubsets[V](indexedData: RDD[(V, Long)], bestSplits:Array[VarSplitInfo], br_subsets:Broadcast[Array[SubsetInfo]], br_splitter:Broadcast[VariableSplitter[V]]) = {
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

  def findBestSplits[V](indexedData: RDD[(V, Long)], br_splits:Broadcast[Array[SubsetInfo]], br_splitter:Broadcast[VariableSplitter[V]])
        (implicit rng:RandomGenerator) =  {
    val seed = rng.nextLong()
    val merger = br_splitter.value.createMerger(seed)
    profIt("REM: findBestSplits") {
      indexedData
        .mapPartitionsWithIndex { case (pi, it) =>
          br_splitter.value.findSplitsForVars(it, br_splits.value)(new XorShift1024StarRandomGenerator(seed ^ pi))
        }.fold(Array.fill(br_splits.value.length)(null))(merger.merge)
    }
  }
}

@SerialVersionUID(1l)
abstract class DecisionTreeNode(val majorityLabel: Int, val size: Int, val nodeImpurity: Double) extends Serializable {
  def isLeaf:Boolean

  def printout(level: Int)
  def impurityContribution:Double = nodeImpurity * size
  def traverse(f:SplitNode=>Boolean):LeafNode = this match {
    case leaf:LeafNode => leaf
    case split:SplitNode => (if (f(split)) split.left else split.right).traverse(f)
  }
  def toStream:Stream[DecisionTreeNode]
  def splitsToStream:Stream[SplitNode]  = toStream.filter(!_.isLeaf).asInstanceOf[Stream[SplitNode]]
  def leafsToStream:Stream[LeafNode]  = toStream.filter(_.isLeaf).asInstanceOf[Stream[LeafNode]]
}

@SerialVersionUID(1l)
case class LeafNode(override val majorityLabel: Int,override val  size: Int,override val  nodeImpurity: Double) extends DecisionTreeNode(majorityLabel,size,nodeImpurity) {
  val isLeaf = true

  def printout(level: Int) {
    print(new String(Array.fill(level)(' ')))
    val nodeType = "leaf"
    println(s"${nodeType}[${majorityLabel}, ${size}, ${nodeImpurity}]")
  }

  override def toString = s"leaf[${majorityLabel}, ${size}, ${nodeImpurity}]"

  def toStream:Stream[DecisionTreeNode] = this #:: Stream.empty
}

object LeafNode {
  def apply(subset:SubsetInfo):LeafNode = apply(subset.majorityLabel, subset.length, subset.impurity)
}

@SerialVersionUID(1l)
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
  override def toString = s"split[${splitVariableIndex}, ${splitPoint}, ${majorityLabel}, ${size}, ${impurityReduction}, ${nodeImpurity}]"

  def childFor(value:Double):DecisionTreeNode =  if (value <= splitPoint) left else right

  // TODO (CHECK): Not sure but this can be the same as impurity reduction
  def impurityDelta  = impurityContribution - (left.impurityContribution + right.impurityContribution)
  def toStream:Stream[DecisionTreeNode] = this #:: left.toStream #::: right.toStream
}

object SplitNode {
  def apply(subset:SubsetInfo, split:VarSplitInfo, left: DecisionTreeNode, right: DecisionTreeNode): SplitNode = apply(subset.majorityLabel, subset.length,  subset.impurity
                 ,split.variableIndex, split.splitPoint, subset.impurity - split.gini, left, right)
}

@SerialVersionUID(1l)
class DecisionTreeModel[V](val rootNode: DecisionTreeNode)(implicit canSplit:CanSplit[V]) extends PredictiveModelWithImportance[V] with  Logging with Serializable {

  def splitVariableIndexes = rootNode.splitsToStream.map(_.splitVariableIndex).toSet

  def predictIndexed(indexedData: RDD[(V,Long)])(implicit ct:ClassTag[V]): Array[Int] = {
    val treeVariableData =  indexedData.collectAtIndexes(splitVariableIndexes)
    Range(0, indexedData.size).map(i => rootNode.traverse(s => canSplit.at(treeVariableData(s.splitVariableIndex))(i) <= s.splitPoint).majorityLabel).toArray
  }

  def printout() {
    rootNode.printout(0)
  }

  def printoutByLevel() {
    def printLevel(levelNodes:Seq[DecisionTreeNode]) {
      if (!levelNodes.isEmpty) {
        println(levelNodes.mkString(" "))
        printLevel(levelNodes.flatMap( _ match {
          case t: SplitNode => List(t.left, t.right)
          case _ => Nil
        }))
      }
    }
    printLevel(Seq(rootNode))
  }


  def variableImportanceAsFastMap: Long2DoubleOpenHashMap = rootNode.splitsToStream.
    foldLeft(new Long2DoubleOpenHashMap()){ case (m, splitNode) =>
      m.increment(splitNode.splitVariableIndex, splitNode.impurityDelta)
    }

  def impurity = rootNode.toStream.map(_.nodeImpurity).toList
  def variables = rootNode.splitsToStream.map(_.splitVariableIndex).toList
  def thresholds = rootNode.splitsToStream.map(_.splitPoint).toList
}


object DecisionTreeModel {

  def resolveSplitNodes[V](indexedData: RDD[(V,Long)], splitNodes:List[(SplitNode, Int)])(implicit canSplit:CanSplit[V]) = {
    val varsAndIndexesToCollect = splitNodes.asInstanceOf[List[(SplitNode, Int)]].map { case (n,i) => (n.splitVariableIndex, i)}.zipWithIndex.toArray
      // broadcast
      val varValuesForSplits = withBroadcast(indexedData)(varsAndIndexesToCollect) { br_varsAndIndexesToCollect =>
        indexedData.mapPartitions{ it =>
          // group by variable index
          val varsAndIndexesToCollectMap = br_varsAndIndexesToCollect.value.toList.groupBy(_._1._1)
          it.flatMap { case (v,vi) =>
            varsAndIndexesToCollectMap.getOrElse(vi, Nil).map { case (n,si) => (si, canSplit.at(v)(n._2)) }
          }
      }.collectAsMap
    }
    splitNodes.asInstanceOf[List[(SplitNode, Int)]].zipWithIndex.map{ case ((n,i),v) => (n.childFor(varValuesForSplits(v)), i)}
  }

  //
  // predict same sample for all trees
  // as the result though I would like to get predictions for each tree not just one so an int[nTrees][nIndexes]
  def batchPredict[V](indexedData: RDD[(V,Long)], trees:Seq[DecisionTreeModel[V]], indexes:Seq[Array[Int]])(implicit canSplit:CanSplit[V]) = {
    // samples are the same so can be broadcasted
    // maybe I can broadcast trees as well ...
    // but in general i should iterate through levels and for each prediction point send out the variable that should be retrieved (and say -1 if none)
    // then I wil get the entire V I can decide the next point

    // use recursion to replace loop
    def predict(nodesAndIndexes:List[((DecisionTreeNode, Int), Int)]):List[((LeafNode, Int), Int)] = {
      val (leaves, splits) = nodesAndIndexes.partition(_._1._1.isLeaf)
      if (splits.isEmpty) {
        leaves.asInstanceOf[List[((LeafNode, Int), Int)]]
      } else {
        val (bareSplits, splitIndexes) = splits.unzip
        val transformedSplits = resolveSplitNodes(indexedData, bareSplits.asInstanceOf[List[(SplitNode, Int)]]).zip(splitIndexes)
        leaves.asInstanceOf[List[((LeafNode, Int), Int)]] ::: predict(transformedSplits)
      }
    }

    val rootNodesAndIndexes = trees.map(_.rootNode).zip(indexes).flatMap { case (n,idx) => idx.map(i => (n,i)) }.zipWithIndex.toList
    val leaveNodesAndIndexes = predict(rootNodesAndIndexes)
    // somehow need to get to the original prediction

    val orderedPredictions = leaveNodesAndIndexes.sortBy(_._2).unzip._1.map(_._1.majorityLabel)
    val orderedPredictionsIter = orderedPredictions.toIterator

    // how to merge it back now?
    // well should be in the same order as original ??? ( NO - the order got corrupted in partitioning process)))
    // so will need to preserve it and sort by or do something else but say it's preserved
    //then I just need to fill the values with an iterator

    //hmm not to sure if map is sequential
    indexes.map(a => Array.fill(a.length)(orderedPredictionsIter.next()))
  }
}

case class DecisionTreeParams(
    val maxDepth:Int = Int.MaxValue,
    val minNodeSize:Int = 1,
    val seed:Long = defRng.nextLong,
    val randomizeEquality:Boolean = false) {

  override def toString = ToStringBuilder.reflectionToString(this)
}

class DecisionTree[V](val params: DecisionTreeParams = DecisionTreeParams())(implicit canSplit:CanSplit[V]) extends Logging with Prof {

  // TODO (Design): This seems like an easiest solution but it make this class
  // to keep random state ... perhaps this could be externalised to the implicit random
  implicit lazy val rnd = new XorShift1024StarRandomGenerator(params.seed)

  def run(data: RDD[V], dataType: VariableType, labels: Array[Int]): DecisionTreeModel[V] = run(data.zipWithIndex(), dataType, labels, 1.0, Sample.all(canSplit.size(data.first())))

  def run(indexedData: RDD[(V, Long)], dataType: VariableType,  labels: Array[Int], nvarFraction: Double, sample:Sample): DecisionTreeModel[V] =
      batchTrain(indexedData, dataType, labels, nvarFraction, List(sample)).head

  /**
   * Trains all the trees for specified samples at the same time
   */
  def batchTrain(indexedData: RDD[(V, Long)], dataType: VariableType,
      labels: Array[Int], nvarFraction: Double, sample:Seq[Sample]): Seq[DecisionTreeModel[V]] = {
    //TODO (OPTIMIZE): Perhaps is't better to use unique indexes and weights
    val splitter = VariableSplitter[V](dataType, labels, nvarFraction, randomizeEquality = params.randomizeEquality)
    val subsets = sample.map { s =>
      val currentSet =  s.indexesIn.toArray
      val (totalGini, totalLabel) = Gini.giniImpurity(currentSet, labels, splitter.nCategories)
      SubsetInfo(currentSet, totalGini, totalLabel)
    }.toList
    val rootNodes = withBroadcast(indexedData)(splitter) { br_splitter =>
      buildSplit(indexedData, subsets, br_splitter, 0)
    }
    rootNodes.map(new DecisionTreeModel[V](_))
  }

  /**
   * Builds (recursively) the decision tree level by level
   */

  def buildSplit(indexedData: RDD[(V, Long)], subsets: List[SubsetInfo], br_splitter: Broadcast[VariableSplitter[V]],treeLevel:Int): List[DecisionTreeNode] = {

    // TODO (Note): This still seem to be somewhat obscure do the the indexing used perhaps now could be done in a single pass
    // but then I do need to aggregate splits for subcalls - so perhaps some indexing is unavoidable
    // unless I do filtering on server side ????

    // for the current set find all candidate splits
    logDebug(s"Building level ${treeLevel}, subsets: ${subsets}")
    profReset()
    // pre-filter the subsets to check if they need further splitting
    // we also need to maintain the original index of the subset so that we can later
    // join the next level tree nodes

    val subsetsToSplit = subsets.zipWithIndex.filter {case (si, _) =>
      si.length >= params.minNodeSize && treeLevel < params.maxDepth
    }
    logDebug(s"Splits to include: ${subsetsToSplit}") 
    
    // TODO: (OPTIMIZE) if there is not splits to calculate do not call compute splits etc.
      
    val (bestSplits, nextLevelSubsets) = findBestSplitsAndSubsets(indexedData, subsetsToSplit.unzip._1.toList, br_splitter)
    logDebug(s"Best splits: ${bestSplits.toList}")
    logDebug(s"Next level splits: ${nextLevelSubsets}")
    
    profPoint("Best splits and splitting done")
 
    // TODO: compute the next level tree nodes (notice the recursive call)
    val nextLevelNodes = if (!nextLevelSubsets.isEmpty) buildSplit(indexedData, nextLevelSubsets, br_splitter, treeLevel + 1) else List()
     
    profPoint("Sublevels done")
    
    val (usefulSplits, usefulSplitsIndices) = bestSplits.zip(subsetsToSplit.unzip._2).filter(_._1 != null).unzip
    // compute the indexes of splitted subsets against the original indexes
    val subsetIndexToSplitIndexMap = usefulSplitsIndices.zipWithIndex.toMap

    // TODO (Optimize): I use oplain arrays that obscure things somewhat assuming they are faster to serialise, but perhaps the difference is negligible and
    // I could use for example an array of tuples rather the obscure indexing scheme

    // I need to be able to find which nodes to use for splits
    // so I need say an Array that would tell me now which nodes were actually passed to split and what their index vas
    // at this stage we would know exactly what we need
     val result = subsets.zipWithIndex.map {case (subset, i) => 
       subsetIndexToSplitIndexMap.get(i)
         .map(splitIndex => SplitNode(subset, usefulSplits(splitIndex),nextLevelNodes(2*splitIndex), nextLevelNodes(2*splitIndex+1)))
         .getOrElse(LeafNode(subset))
     }.toList
     profPoint("building done")
     
    result 
  }
  
  def findBestSplitsAndSubsets(indexedData: RDD[(V, Long)], subsetsToSplit:List[SubsetInfo], br_splitter:Broadcast[VariableSplitter[V]]) =  {
    profIt("findBestSplitsAndSubsets") { 
      val subsetsToSplitAsIndices = subsetsToSplit.toArray
      withBroadcast(indexedData)(subsetsToSplitAsIndices){ br_splits => 
        val bestSplits = DecisionTree.findBestSplits(indexedData, br_splits, br_splitter)

        // now with the same broadcasted splits

        (bestSplits, DecisionTree.splitSubsets(indexedData, bestSplits, br_splits, br_splitter))
      }
    }
  }
}




