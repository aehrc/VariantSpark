package au.csiro.variantspark.algo

import au.csiro.pbdava.ssparkle.common.utils.FastUtilConversions._
import au.csiro.pbdava.ssparkle.common.utils.{Logging, Prof}
import au.csiro.pbdava.ssparkle.spark.SparkUtils._
import au.csiro.variantspark.data.{DataBuilder, DataLike, Feature, StdFeature, VariableType}
import au.csiro.variantspark.metrics.Gini
import au.csiro.variantspark.utils.IndexedRDDFunction._
import au.csiro.variantspark.utils._
import it.unimi.dsi.fastutil.longs.{Long2DoubleOpenHashMap, Long2LongOpenHashMap}
import it.unimi.dsi.util.XorShift1024StarRandomGenerator
import org.apache.commons.lang3.builder.ToStringBuilder
import org.apache.commons.math3.random.RandomGenerator
import org.apache.commons.math3.util.MathArrays
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

/** Allows for a general description of the construct
  *
  * Specify the 'indices', 'impurtity', and 'majoritylabel' these values will not be visible
  * outside the class
  *
  * {{{
  * val airSubInfo = SubsetInfo(indices, impurtity)
  * }}}
  *
  * @constructor creates value based on the indices, and impurity
  * @param indices: input an array of integers representing the indices of the values
  * @param impurity: input the value of impurity of the data construct
  */
case class RegressionSubsetInfo(indices: Array[Int], impurity: Double) {

  /** An alternative constructor for the SubsetInfo class, use this if the majorityLabel has not
    * already been defined
    *
    * Specify the 'indices', 'impurity', 'labels', and 'nLabels'
    *
    * {{{
    * val subInfo = SubsetInfo(indices, impurity, labels, nLables)
    * }}}
    *
    * @param indices: input an array of integers that contains the indices required
    * @param impurity: a value based on the gini impurity of the dataset sent in
    * @param labels: in put an array of integers that contains the labels of the values for each row
    * @param nLabels: specify the number of labels for that specific dataset
    *
    */
  // def this(indices: Array[Int], impurity: Double, labels: Array[Int], nLabels: Int) {
  //   this(indices, impurity, FactorVariable.classCounts(indices, labels, nLabels))
  // }

  def length: Int = indices.length
  override def toString: String = s"SubsetInfo(${indices.toList},${impurity}"
}

/** Class utilized to give an insight into the split data
  *
  * Specify the 'variableIndex', 'splitPoint', 'gini', 'leftGini', and 'rightGini'
  *
  * @constructor creates information about the split that occured on a specifc variable
  * @param variableIndex : specifies the index of the variable that the dataset will
  *                      split on
  * @param splitPoint    : specifies the point in the index of the exact split
  * @param gini          : general gini value of the dataset
  * @param leftGini      : the gini impurity of the left split of the dataset
  * @param rightGini     : the gini impurity of the right split of the dataset
  */
case class RegressionVarSplitInfo(variableIndex: Long, splitPoint: Double, impurity: Double,
    leftImpurity: Double, rightImpurity: Double, isPermutated: Boolean) {

  /** Creates a list of the subsetInfos for the dataset split
    *
    * @param v           : input the specific data construct
    * @param labels      : input an array of integer labels
    * @param nCategories : specify the number of categories of the dataset or 'columns'
    * @param subset      : specify the SubsetInfo class touched on previously at
    *                    [[au.csiro.variantspark.algo.SubsetInfo]]
    * @return returns a tupple of the subset information
    */
  def split(v: TreeFeature, labels: Array[Double])(
      subset: RegressionSubsetInfo): (RegressionSubsetInfo, RegressionSubsetInfo) = {
    (new RegressionSubsetInfo(subset.indices.filter(v.at(_) <= splitPoint), leftImpurity),
      new RegressionSubsetInfo(subset.indices.filter(v.at(_) > splitPoint), rightImpurity))
  }

  def splitPermutated(v: TreeFeature, labels: Array[Double], permutationOder: Array[Int])(
      subset: RegressionSubsetInfo): (RegressionSubsetInfo, RegressionSubsetInfo) = {
    (new RegressionSubsetInfo(subset.indices.filter(i => v.at(permutationOder(i)) <= splitPoint),
        leftImpurity),
      new RegressionSubsetInfo(subset.indices.filter(i => v.at(permutationOder(i)) > splitPoint),
        rightImpurity))
  }
}

/** Utilized to return a VarSplitInfo object
  */
object RegressionVarSplitInfo {

  /** Applies the values obtained from the [[au.csiro.variantspark.algo.SplitInfo]] class to
    * create an [[au.csiro.variantspark.algo.VarSplitInfo]] object
    *
    * @param variableIndex  : input an index of where the variable split from
    * @param split                  : input a [[au.csiro.variantspark.algo.SplitInfo]] object
    * @return returns a [[au.csiro.variantspark.algo.VarSplitInfo]] object
    */
  def apply(variableIndex: Long, split: SplitInfo,
      isPermuted: Boolean): RegressionVarSplitInfo =
    apply(variableIndex, split.splitPoint, split.gini, split.leftGini, split.rightGini,
      isPermuted) // TODO: Decided to use existing SpitInfo even though the naming is not
  // correct here
}

/** Defines the trait for the case class [[au.csiro.variantspark.algo.DeterministicMerger]]
  */
trait RegressionMerger {

  /** Operates a merging function utilizing two arrays of the
    * class [[au.csiro.variantspark.algo.VarSplitInfo]]
    *
    * @param a1: input an array of [[au.csiro.variantspark.algo.VarSplitInfo]]
    * @param a2: input an array of [[au.csiro.variantspark.algo.VarSplitInfo]]
    * @return Returns an array of [[au.csiro.variantspark.algo.VarSplitInfo]]
    */
  def merge(a1: Array[RegressionVarSplitInfo],
      a2: Array[RegressionVarSplitInfo]): Array[RegressionVarSplitInfo]
}

/** Utilizes the Deterministic Decision Tree model found here:
  * [[https://en.wikipedia.org/wiki/Decision_tree_model#Randomized_decision_tree]]
  * Extends the [[au.csiro.variantspark.algo.Merger]] class
  */
case class RegressionDeterministicMerger() extends RegressionMerger {

  /** Operates a merging function utilizing two arrays of the
    * class [[au.csiro.variantspark.algo.VarSplitInfo]]
    *
    * @param a1: input an array of [[au.csiro.variantspark.algo.VarSplitInfo]]
    * @param a2: input an array of [[au.csiro.variantspark.algo.VarSplitInfo]]
    * @return Returns the merged array a1
    */
  def merge(a1: Array[RegressionVarSplitInfo],
      a2: Array[RegressionVarSplitInfo]): Array[RegressionVarSplitInfo] = {

    /** Takes the [[au.csiro.variantspark.algo.VarSplitInfo]] from two seperate splits
      * and returns the value from either s1 or s2 based on the gini impurity
      *
      * @param s1: input an [[au.csiro.variantspark.algo.VarSplitInfo]]
      * @param s2: input an [[au.csiro.variantspark.algo.VarSplitInfo]]
      * @return Returns either s1 or s2 based on the gini impurity calculation
      */
    def mergeSplitInfo(s1: RegressionVarSplitInfo, s2: RegressionVarSplitInfo) = {
      if (s1 == null) s2
      else if (s2 == null) s1
      else if (s1.impurity < s2.impurity) s1
      else if (s2.impurity < s1.impurity) s2
      else if (s1.variableIndex < s2.variableIndex) s1
      else s2
    }
    a1.indices.foreach(i => a1(i) = mergeSplitInfo(a1(i), a2(i)))
    a1
  }
}

/**
  *  UsedsMurmur3 hashing to create random ordering of variables
  *  dependent on the initial seed and split number.
  *
  *  The assumption is that comparing the hashes of variable indexes will produce
  *  sufficently randomzized orderings given different seeds and split ids.
  *
  *   @param seed: input a seed value to initialize the random number generator for rnd
  */
case class RegressionRandomizingMergerMurmur3(seed: Long) extends RegressionMerger {

  def hashOrder(varIndex: Long, splitId: Int): Int = {
    MurMur3Hash.hashLong(varIndex, MurMur3Hash.hashLong(seed, splitId))
  }

  def chooseEqual(s1: RegressionVarSplitInfo, s2: RegressionVarSplitInfo,
      id: Int): RegressionVarSplitInfo = {
    if (hashOrder(s1.variableIndex, id) < hashOrder(s2.variableIndex, id)) s1 else s2
  }

  /** Operates a merging function utilizing two arrays of the
    * class [[au.csiro.variantspark.algo.VarSplitInfo]]
    *
    * @param a1: input an array of [[au.csiro.variantspark.algo.VarSplitInfo]]
    * @param a2: input an array of [[au.csiro.variantspark.algo.VarSplitInfo]]
    * @return Returns the merged array a1
    */
  def merge(a1: Array[RegressionVarSplitInfo],
      a2: Array[RegressionVarSplitInfo]): Array[RegressionVarSplitInfo] = {

    /** Takes the [[au.csiro.variantspark.algo.VarSplitInfo]] from two seperate splits
      * and returns the value from either s1 or s2 based on the gini impurity
      *
      * @note if the gini values of each split are equal then the value returns one at random
      *
      * @param s1: input an [[au.csiro.variantspark.algo.VarSplitInfo]]
      * @param s2: input an [[au.csiro.variantspark.algo.VarSplitInfo]]
      * @return Returns either s1 or s2 based on the gini impurity calculation
      */
    def mergeSplitInfo(s1: RegressionVarSplitInfo, s2: RegressionVarSplitInfo, id: Int) = {
      if (s1 == null) s2
      else if (s2 == null) s1
      else if (s1.impurity < s2.impurity) s1
      else if (s2.impurity < s1.impurity) s2
      else chooseEqual(s1, s2, id)
    }
    a1.indices.foreach(i => a1(i) = mergeSplitInfo(a1(i), a2(i), i))
    a1
  }
}

trait RegressionVariableSplitter {

  def initialSubset(sample: Sample): RegressionSubsetInfo

  /** Splits the subsets of the RDD and returns a split based on the variable of split index
    *
    * @param varData: input an interator containing the dataset and an index
    * @param subsets: input an array of [[au.csiro.variantspark.algo.SubsetInfo]]
    * @param bestSplits: input an array of the [[au.csiro.variantspark.algo.VarSplitInfo]]
    * @return returns a flattened iterator
    */
  def splitSubsets(varData: Iterator[TreeFeature], subsets: Array[RegressionSubsetInfo],
      bestSplits: Array[RegressionVarSplitInfo])
      : Iterator[(Int, (RegressionSubsetInfo, RegressionSubsetInfo))]
  def findSplitsForVars(varData: Iterator[TreeFeature], splits: Array[RegressionSubsetInfo])(
      implicit rng: RandomGenerator): Iterator[Array[RegressionVarSplitInfo]]
  def createMerger(seed: Long): RegressionMerger
}

/** This is the main split function
  *
  * 1. specifies the number of categories based on the label input
  * 2. Finds the splits in the data based on the gini value
  *
  * @param labels: input an array of labels used by the dataset
  * @param mTryFraction:  the fraction of variable to try at each split (default to 1.0)
  * @param randomizeEquality: default to false
  */
case class RegressionAirVariableSplitter(labels: Array[Double], permutationOrder: Array[Int],
    mTryFraction: Double, randomizeEquality: Boolean)
    extends RegressionVariableSplitter with Logging with Prof {

  lazy val permutedLabels: Array[Double] = permutationOrder.map(labels(_))

  def initialSubset(sample: Sample): RegressionSubsetInfo = {
    val currentSet = sample.indexes
    // val (totalGini, classCounts) = Gini.giniImpurity(currentSet, labels, nCategories) // TODO
    val totalImpurity = VarianceImpurity.createAggregator().getValue // TODO: Will it work?
    RegressionSubsetInfo(currentSet, totalImpurity)
  }

  /** Find the splits in the data based on the gini value
    *
    * Specify the 'data' and 'splits' inputs
    *
    * @param typedData: input the data from the dataset of generic type V
    * @param splits: input an array of the [[au.csiro.variantspark.algo.SubsetInfo]] class
    * @return returns an array [[au.csiro.variantspark.algo.SplitInfo]]
    */
  def findSplits(typedData: TreeFeature, splits: Array[RegressionSubsetInfo],
      sbf: RegressionSplitterFactory, permutedSbf: RegressionSplitterFactory,
      permSubsets: Array[Array[Int]])(
      implicit rng: RandomGenerator): Array[RegressionVarSplitInfo] = {

    val splitter = sbf.create(typedData)
    val permutedSplitter = permutedSbf.create(typedData)

    splits.zip(permSubsets).map {
      case (subsetInfo, permIndexes) =>
        val rnd = rng.nextDouble()
        if (rnd <= mTryFraction) {
          // check whether to use informative or permuted labels
          val permuted = rnd > mTryFraction / 2
          val selectedSplitter = if (!permuted) splitter else permutedSplitter
          val indices = if (!permuted) subsetInfo.indices else permIndexes
          val splitInfo = selectedSplitter.findSplit(indices)
          if (splitInfo != null && splitInfo.gini < subsetInfo.impurity) { // gini NAME is right
            RegressionVarSplitInfo(typedData.index, splitInfo, permuted)
          } else { null }
        } else null
    }
  }

  def threadSafeSplitterBuilderFactory(labels: Array[Double]): RegressionSplitterFactory = {
    // TODO: this should be actually passed externally (or at least part of it
    // as it essentially determines what kind of tree are we building (e.g what is the metric
    // used for impurity)
    // This should obtain a stateful and somehow compartmentalised impurity calculator
    // (Try thread local perhaps here, but creating a new one (per partition) also fits the bill)
    new RegressionSplitterFactory(VarianceImpurity, labels)
  }

  /** Returns the result of a split based on a variable
    *
    * @param varData: input an Iterator of a tuple containing the dataset and indices
    * @param splits: input an Array of the [[au.csiro.variantspark.algo.SubsetInfo]] class
    * @return takes the varData and maps the value of the dataset
    */
  def findSplitsForVars(varData: Iterator[TreeFeature], splits: Array[RegressionSubsetInfo])(
      implicit rng: RandomGenerator): Iterator[Array[RegressionVarSplitInfo]] = {
    profIt("Local: splitting") {
      val sbf = threadSafeSplitterBuilderFactory(labels)
      val permutedSbf = threadSafeSplitterBuilderFactory(permutedLabels)

      // TODO: [Performance] maybe there is not need to permute all the splits up front

      val permSubsets = splits.map(s => ArraysUtils.permutate(s.indices, permutationOrder))
      varData.map(vi => findSplits(vi, splits, sbf, permutedSbf, permSubsets))
    }
  }

  /** Splits the subsets of the RDD and returns a split based on the variable of split index
    *
    * @param varData: input an interator containing the dataset and an index
    * @param subsets: input an array of [[au.csiro.variantspark.algo.SubsetInfo]]
    * @param bestSplits: input an array of the [[au.csiro.variantspark.algo.VarSplitInfo]]
    * @return returns a flattened iterator
    */
  def splitSubsets(varData: Iterator[TreeFeature], subsets: Array[RegressionSubsetInfo],
      bestSplits: Array[RegressionVarSplitInfo])
      : Iterator[(Int, (RegressionSubsetInfo, RegressionSubsetInfo))] = {

    val usefulSubsetSplitAndIndex =
      subsets.zip(bestSplits).filter(_._2 != null).zipWithIndex.toList
    val splitByVarIndex = usefulSubsetSplitAndIndex.groupBy(_._1._2.variableIndex)
    varData.flatMap { vi =>
      splitByVarIndex.getOrElse(vi.index, Nil).map {
        case ((subsetInfo, splitInfo), si) =>
          if (!splitInfo.isPermutated) {
            (si, splitInfo.split(vi, labels)(subsetInfo))
          } else {
            (si, splitInfo.splitPermutated(vi, labels, permutationOrder)(subsetInfo))
          }
      }
    }
  }

  def createMerger(seed: Long): RegressionMerger =
    if (randomizeEquality) RegressionRandomizingMergerMurmur3(seed)
    else RegressionDeterministicMerger()

}

object RegressionAirVariableSplitter {
  def apply(labels: Array[Double], seed: Long, mTryFraction: Double = 1.0,
      randomizeEquality: Boolean = false): RegressionAirVariableSplitter = {
    val rng = new XorShift1024StarRandomGenerator(seed)
    val permutationOrder = labels.indices.toArray
    MathArrays.shuffle(permutationOrder, rng)
    RegressionAirVariableSplitter(labels, permutationOrder, mTryFraction, randomizeEquality)
  }
}

/** Object utilized with the DecisionTreeModel class
  */
object RegressionDecisionTree extends Logging with Prof {

  /** Returns the splitted subsets input through the indexedData param and outputs a list of
    * the Splitted Subsets
    *
    * @param indexedData: input an RDD of the dataset plus indexes of type long
    * @param bestSplits: input an Array containing
    *                  the [[au.csiro.variantspark.algo.VarSplitInfo]] class
    * @param br_subsets: input a Broadcast of Arrays containing
    *                  the [[au.csiro.variantspark.algo.SubsetInfo]] class
    * @param br_splitter: Broadcast of the [[au.csiro.variantspark.algo.VariableSplitter]] class
    * @return Returns an indexed list of splited subsets
    */
  def splitSubsets(indexedData: RDD[TreeFeature], bestSplits: Array[RegressionVarSplitInfo],
      br_subsets: Broadcast[Array[RegressionSubsetInfo]],
      br_splitter: Broadcast[RegressionVariableSplitter]): List[RegressionSubsetInfo] = {
    profIt("REM: splitSubsets") {
      val indexedSplittedSubsets = withBroadcast(indexedData)(bestSplits) { br_bestSplits =>
        // format: off
        indexedData
          .mapPartitions(it => br_splitter.value.splitSubsets(it, br_subsets.value,
            br_bestSplits.value))
          .collectAsMap()
        // format: on
      }
      indexedSplittedSubsets
        .foldLeft(Array.fill[RegressionSubsetInfo](indexedSplittedSubsets.size * 2)(null)) {
          case (a, (i, st)) =>
            a(2 * i) = st._1
            a(2 * i + 1) = st._2
            a
        }
        .toList
    }
  }

  /** Returns an indexed
    *
    * @param treeFeatures: input an RDD of tree features
    * @param br_splits: input a broadcast containing an array of
    *                 the [[au.csiro.variantspark.algo.SubsetInfo]] class
    * @param br_splitter: input a broadcast containing
    *                   the [[au.csiro.variantspark.algo.VariableSplitter]] class of the dataset
    * @return Returns the indexedData variable that contains the indexed best splits
    */
  def findBestSplits(treeFeatures: RDD[TreeFeature],
      br_splits: Broadcast[Array[RegressionSubsetInfo]],
      br_splitter: Broadcast[RegressionVariableSplitter])(
      implicit rng: RandomGenerator): Array[RegressionVarSplitInfo] = {
    val seed = rng.nextLong()
    val merger = br_splitter.value.createMerger(seed)
    profIt("REM: findBestSplits") {
      treeFeatures
        .mapPartitionsWithIndex {
          case (pi, it) =>
            br_splitter.value.findSplitsForVars(it,
              br_splits.value)(new XorShift1024StarRandomGenerator(seed ^ pi))
        }
        .fold(Array.fill(br_splits.value.length)(null))(merger.merge)
    }
  }
}

@SerialVersionUID(1L)
abstract class RegressionDecisionTreeNode(val meanLabel: Int, val size: Int,
    val nodeImpurity: Double)
    extends Serializable {

  def isLeaf: Boolean
  def printout(level: Int)
  def impurityContribution: Double = nodeImpurity * size
  def traverse(f: RegressionSplitNode => Boolean): RegressionLeafNode = this match {
    case leaf: RegressionLeafNode => leaf
    case split: RegressionSplitNode => (if (f(split)) split.left else split.right).traverse(f)
  }
  def toStream: Stream[RegressionDecisionTreeNode]
  def splitsToStream: Stream[RegressionSplitNode] =
    toStream.filter(!_.isLeaf).asInstanceOf[Stream[RegressionSplitNode]]
  def leafsToStream: Stream[RegressionLeafNode] =
    toStream
      .filter(_.isLeaf)
      .asInstanceOf[Stream[RegressionLeafNode]]
}

@SerialVersionUID(1L)
case class RegressionLeafNode(override val meanLabel: Int, override val size: Int,
    override val nodeImpurity: Double)
    extends RegressionDecisionTreeNode(meanLabel, size, nodeImpurity) {
  val isLeaf: Boolean = true

  def printout(level: Int) {
    print(new String(Array.fill(level)(' ')))
    val nodeType = "leaf"
    println(s"${nodeType}[${meanLabel}, ${size}, ${nodeImpurity}]")
  }

  override def toString: String = s"leaf[${meanLabel}, ${size}, ${nodeImpurity}]"

  def toStream: Stream[RegressionDecisionTreeNode] = this #:: Stream.empty
}

object RegressionLeafNode {
  def apply(subset: RegressionSubsetInfo): RegressionLeafNode =
    apply(subset.meanLabel, subset.length, subset.impurity) // TODO

  /**
    * Create a tree leaf for a standard classifier tree with voting information onlu
    * @param majorityLabel
    * @param size
    * @param nodeImpurity
    * @return
    */
  def voting(meanLabel: Int, size: Int, nodeImpurity: Double): RegressionLeafNode = {
    RegressionLeafNode(meanLabel, null, size, nodeImpurity) // TODO
  }

}

@SerialVersionUID(1L)
case class RegressionSplitNode(override val meanLabel: Int, override val size: Int,
    override val nodeImpurity: Double, splitVariableIndex: Long, splitPoint: Double,
    impurityReduction: Double, left: RegressionDecisionTreeNode,
    right: RegressionDecisionTreeNode, isPermuted: Boolean = false)
    extends RegressionDecisionTreeNode(meanLabel, size, nodeImpurity) {

  val isLeaf: Boolean = false

  def printout(level: Int) {
    print(new String(Array.fill(level)(' ')))
    val nodeType = "split"
    println(
        s"${nodeType}[${splitVariableIndex}, ${splitPoint}, ${meanLabel},"
          + s" ${size}, ${impurityReduction}, ${nodeImpurity}]")
    left.printout(level + 1)
    right.printout(level + 1)
  }
  override def toString: String =
    (s"split[${splitVariableIndex}, ${splitPoint}, ${meanLabel}, ${size},"
      + s" ${impurityReduction}, ${nodeImpurity}]")

  def childFor(value: Double): RegressionDecisionTreeNode =
    if (value <= splitPoint) left else right

  def impurityDelta: Double = {
    val deltaAbs = impurityContribution - (left.impurityContribution + right.impurityContribution)
    if (isPermuted) -deltaAbs else deltaAbs
  }
  def toStream: Stream[RegressionDecisionTreeNode] = this #:: left.toStream #::: right.toStream
}

object RegressionSplitNode {
  def apply(subset: RegressionSubsetInfo, split: RegressionVarSplitInfo,
      left: RegressionDecisionTreeNode, right: RegressionDecisionTreeNode): SplitNode =
    apply(subset.meanLabel, subset.length, subset.impurity, split.variableIndex, split.splitPoint,
      subset.impurity - split.impurity, left, right, split.isPermutated)

  def voting(majorityLabel: Int, size: Int, nodeImpurity: Double, splitVariableIndex: Long,
      splitPoint: Double, impurityReduction: Double, left: DecisionTreeNode,
      right: DecisionTreeNode, isPermuted: Boolean = false): SplitNode = {
    SplitNode(majorityLabel, null, size, nodeImpurity, splitVariableIndex, splitPoint,
      impurityReduction, left, right, isPermuted)
  }

}

@SerialVersionUID(1L)
case class RegressionDecisionTreeModel(rootNode: RegressionDecisionTreeNode)
    extends PredictiveModelWithImportance with Logging with Serializable {

  def splitVariableIndexes: Set[Long] = rootNode.splitsToStream.map(_.splitVariableIndex).toSet

  def predict[T](indexedData: RDD[(T, Long)], variableType: VariableType)(
      implicit db: DataBuilder[T]): Array[Int] = {
    predict(indexedData.map({ case (v, i) => (StdFeature.from(null, variableType, v), i) }))
  }

  def predict(indexedData: RDD[(Feature, Long)]): Array[Int] = {
    val treeVariableData = indexedData.collectAtIndexes(splitVariableIndexes)
    Range(0, indexedData.size)
      .map(i =>
          rootNode
            .traverse(s => treeVariableData(s.splitVariableIndex).at(i) <= s.splitPoint)
            .meanLabel)
      .toArray
  }

  def printout() {
    rootNode.printout(0)
  }

  def printoutByLevel() {
    @scala.annotation.tailrec
    def printLevel(levelNodes: Seq[RegressionDecisionTreeNode]) {
      if (levelNodes.nonEmpty) {
        println(levelNodes.mkString(" "))
        printLevel(levelNodes.flatMap(_ match {
          case t: RegressionSplitNode => List(t.left, t.right)
          case _ => Nil
        }))
      }
    }
    printLevel(Seq(rootNode))
  }

  override def variableImportanceAsFastMap: Long2DoubleOpenHashMap = {
    rootNode.splitsToStream.foldLeft(new Long2DoubleOpenHashMap()) {
      case (m, splitNode) =>
        m.increment(splitNode.splitVariableIndex, splitNode.impurityDelta)
    }
  }

  override def variableSplitCountAsFastMap: Long2LongOpenHashMap = {
    rootNode.splitsToStream.foldLeft(new Long2LongOpenHashMap()) {
      case (m, splitNode) =>
        m.increment(splitNode.splitVariableIndex, 1L)
    }
  }

  def impurity: List[Double] = rootNode.toStream.map(_.nodeImpurity).toList
  def variables: List[Long] = rootNode.splitsToStream.map(_.splitVariableIndex).toList
  def thresholds: List[Double] = rootNode.splitsToStream.map(_.splitPoint).toList

}

/** Contains the object for the [[au.csiro.variantspark.algo.DecisionTreeModel]] class
  */
object RegressionDecisionTreeModel {

  /** Returns the resolved list of the split nodes and indices
    *
    * @param indexedData: input an RDD of tuples with the valeus in the dataset and the index
    * @param splitNodes: input a list of tuples with the
    *                  [[au.csiro.variantspark.algo.SplitNode]] class and an index
    * @return returns a List of the resolved [[au.csiro.variantspark.algo.SplitNode]]
    *         class and it's index
    */
  def resolveSplitNodes(indexedData: RDD[(DataLike, Long)],
      splitNodes: List[(RegressionSplitNode, Int)]): List[(RegressionDecisionTreeNode, Int)] = {
    val varsAndIndexesToCollect = splitNodes
      .asInstanceOf[List[(RegressionSplitNode, Int)]]
      .map { case (n, i) => (n.splitVariableIndex, i) }
      .zipWithIndex
      .toArray
    val varValuesForSplits = withBroadcast(indexedData)(varsAndIndexesToCollect) {
      br_varsAndIndexesToCollect =>
        indexedData.mapPartitions { it =>
          val varsAndIndexesToCollectMap =
            br_varsAndIndexesToCollect.value.toList.groupBy(_._1._1)
          it.flatMap {
            case (v, vi) =>
              varsAndIndexesToCollectMap.getOrElse(vi, Nil).map {
                case (n, si) => (si, v.at(n._2))
              }
          }
        }.collectAsMap
    }
    splitNodes.asInstanceOf[List[(RegressionSplitNode, Int)]].zipWithIndex.map {
      case ((n, i), v) => (n.childFor(varValuesForSplits(v)), i)
    }
  }

  def batchPredict(indexedData: RDD[(DataLike, Long)], trees: Seq[RegressionDecisionTreeModel],
      indexes: Seq[Array[Int]]): Seq[Array[Int]] = {

    /** Takes the decision tree nodes and outputs the leaf nodes
      * Partitions the nodesAndIndexes variable and recursively iterates through each
      * model until a leaf node is reached
      *
      * @param nodesAndIndexes: input a list of tuples of tuple
      * @return a list of tuples of tuple
      */
    def predict(nodesAndIndexes: List[((RegressionDecisionTreeNode, Int), Int)])
        : List[((RegressionLeafNode, Int), Int)] = {
      val (leaves, splits) = nodesAndIndexes.partition(_._1._1.isLeaf)
      if (splits.isEmpty) {
        leaves.asInstanceOf[List[((RegressionLeafNode, Int), Int)]]
      } else {
        val (bareSplits, splitIndexes) = splits.unzip
        val transformedSplits =
          resolveSplitNodes(indexedData,
            bareSplits.asInstanceOf[List[(RegressionSplitNode, Int)]])
            .zip(splitIndexes)
        leaves.asInstanceOf[List[((RegressionLeafNode, Int), Int)]] ::: predict(transformedSplits)
      }
    }

    val rootNodesAndIndexes = trees
      .map(_.rootNode)
      .zip(indexes)
      .flatMap { case (n, idx) => idx.map(i => (n, i)) }
      .zipWithIndex
      .toList
    val leaveNodesAndIndexes = predict(rootNodesAndIndexes)

    val orderedPredictions = leaveNodesAndIndexes.sortBy(_._2).map(_._1).map(_._1.meanLabel)
    val orderedPredictionsIter = orderedPredictions.toIterator

    indexes.map(a => Array.fill(a.length)(orderedPredictionsIter.next()))
  }
}

/** Class for the Decision tree model
  *
  * Specify the 'params' using the [[au.csiro.variantspark.algo.DecisionTreeParams]]
  *
  * {{{
  *
  * val maxDepth = 5
  * val minNodeSize = 10
  * val seed = 1
  * val randomizeEquality = false
  *
  * val params = DecisionTreeParams(maxDepth, minNodeSize, seed, randomizeEquality)
  * val model = DecisionTree(params)
  *
  * }}}
  *
  * @param params: input the [[au.csiro.variantspark.algo.DecisionTreeParams]] class
  *              containing the main aspects of the model
  */
class RegressionDecisionTree(val params: DecisionTreeParams = DecisionTreeParams(),
    val trf: TreeRepresentationFactory = DefTreeRepresentationFactory)
    extends Logging with Prof {

  implicit lazy val rnd: XorShift1024StarRandomGenerator =
    new XorShift1024StarRandomGenerator(params.seed)

  implicit def toRepresenation(indexedFeatures: RDD[(Feature, Long)]): RDD[TreeFeature] =
    trf.createRepresentation(indexedFeatures)

  /** Basic training operation taking the in the data, the type, and the labels
    *
    * @param indexedData: input an RDD of the dataset
    * @param labels: input an array of integers changed to an integer representation
    */
  def train(indexedData: RDD[(Feature, Long)],
      labels: Array[Double]): RegressionDecisionTreeModel =
    train(indexedData, labels, 1.0, Sample.all(indexedData.first._1.size))

  /** Alternative train function
    *
    * @param indexedData: input an RDD of the values of the dataset with the indices
    * @param labels: input an array of integers changed to an integer representation
    * @param nvarFraction: fraction of variable to test at each split
    * @param sample: input the [[au.csiro.variantspark.utils.Sample]] class that
    *              contains the size and the indices
    */
  def train(indexedData: RDD[(Feature, Long)], labels: Array[Double], nvarFraction: Double,
      sample: Sample): RegressionDecisionTreeModel =
    batchTrain(indexedData, labels, nvarFraction, List(sample)).head

  /** Trains all the trees for specified samples at the same time
    *
    * @param indexedFeatures: input an RDD of the values of the dataset with the indices
    * @param labels: input an array of integers changed to an integer representation
    * @param nvarFraction: fraction of variable to test for each split
    * @param sample: input the [[au.csiro.variantspark.utils.Sample]] class that
    *              contains the size and the indices
    * @return Returns a Sequence of [[au.csiro.variantspark.algo.DecisionTreeModel]]
    *         classes containing the dataset
    */
  def batchTrain(indexedFeatures: RDD[(Feature, Long)], labels: Array[Double],
      nvarFraction: Double, sample: Seq[Sample]): Seq[DecisionTreeModel] = {
    batchTrainInt(trf.createRepresentation(indexedFeatures), labels, nvarFraction, sample)
  }

  /** Trains all the trees for specified samples at the same time
    *
    * @param features: input an RDD of the internal tree feature representation
    * @param labels: input an array of integers changed to an integer representation
    * @param nvarFraction: fraction of variable to test for each split
    * @param sample: input the [[au.csiro.variantspark.utils.Sample]] class that
    *              contains the size and the indices
    * @return Returns a Sequence of [[au.csiro.variantspark.algo.DecisionTreeModel]]
    *         classes containing the dataset
    */
  def batchTrainInt(features: RDD[TreeFeature], labels: Array[Double], nvarFraction: Double,
      sample: Seq[Sample]): Seq[RegressionDecisionTreeModel] = {

    // manage persistence here - cache the features if not already cached
    withCached(features) { cachedFeatures =>
      val splitter: RegressionAirVariableSplitter =
        RegressionAirVariableSplitter(labels,
          if (params.airRandomSeed != 0L) params.airRandomSeed else params.seed, nvarFraction,
          randomizeEquality = params.randomizeEquality)
      val subsets = sample.map(splitter.initialSubset).toList
      val rootNodes = withBroadcast(cachedFeatures)(splitter) { br_splitter =>
        buildSplit(cachedFeatures, subsets, br_splitter, 0)
      }
      rootNodes.map(new RegressionDecisionTreeModel(_))
    }
  }

  private def summarize(subsets: List[RegressionSubsetInfo]): String = {
    s"#${subsets.size} => ${subsets.map(_.length)}"
  }

  /** Builds (recursively) the decision tree level by level
    *
    * @param indexedTypedData: input an RDD of tree features
    * @param subsets: input an Array containing the [[au.csiro.variantspark.algo.VarSplitInfo]]
    *               class
    * @param br_splitter: input a Broadcast of Arrays containing
    *                   the [[au.csiro.variantspark.algo.SubsetInfo]] class
    * @param treeLevel: specify the current level of the tree being built
    * @return Returns a subset of the splits
    */
  private def buildSplit(indexedTypedData: RDD[TreeFeature], subsets: List[RegressionSubsetInfo],
      br_splitter: Broadcast[RegressionVariableSplitter],
      treeLevel: Int): List[RegressionDecisionTreeNode] = {

    logDebug(s"Building level ${treeLevel}")
    logDebug(s"Initial subsets: ${summarize(subsets)}")
    logTrace(s"Initial subsets (details): ${subsets}")

    profReset()

    val subsetsToSplit = subsets.zipWithIndex.filter {
      case (si, _) =>
        si.length >= params.minNodeSize && treeLevel < params.maxDepth
    }
    logDebug(s"Splittable subsets: ${summarize(subsetsToSplit.map(_._1))}")
    logTrace(s"Splittable subsets (details): ${subsetsToSplit}")

    val (bestSplits, nextLevelSubsets) =
      findBestSplitsAndSubsets(indexedTypedData, subsetsToSplit.map(_._1), br_splitter)
    logDebug(s"Best splits: ${bestSplits.toList}")
    logDebug(s"Next level subsets ${summarize(nextLevelSubsets)}")
    logTrace(s"Next level subsets (details): ${nextLevelSubsets}")

    profPoint("Best splits and splitting done")

    val nextLevelNodes =
      if (nextLevelSubsets.nonEmpty) {
        buildSplit(indexedTypedData, nextLevelSubsets, br_splitter, treeLevel + 1)
      } else { List() }

    profPoint("Sublevels done")

    val (usefulSplits, usefulSplitsIndices) =
      bestSplits.zip(subsetsToSplit.map(_._2)).filter(_._1 != null).unzip
    val subsetIndexToSplitIndexMap = usefulSplitsIndices.zipWithIndex.toMap
    val result = subsets.zipWithIndex.map {
      case (subset, i) =>
        // format: off
        subsetIndexToSplitIndexMap
          .get(i)
          .map(splitIndex => RegressionSplitNode(subset, usefulSplits(splitIndex),
            nextLevelNodes(2 * splitIndex), nextLevelNodes(2 * splitIndex + 1)))
          .getOrElse(RegressionLeafNode(subset))
      // format: on
    }
    profPoint("building done")

    result
  }

  /** Finds the best split using the [[au.csiro.variantspark.algo.DecisionTree]]
    * class's findBestSplits function then broadcast to the bestSplits variable
    *
    * @param treeFeatures: input an RDD of tree freatures
    * @param subsetsToSplit: input a list of [[au.csiro.variantspark.algo.SubsetInfo]]
    * @param br_splitter: input a Broadcast of Arrays containing
    *                   the [[au.csiro.variantspark.algo.SubsetInfo]] class
    */
  private def findBestSplitsAndSubsets(treeFeatures: RDD[TreeFeature],
      subsetsToSplit: List[RegressionSubsetInfo],
      br_splitter: Broadcast[RegressionVariableSplitter]) = {
    profIt("findBestSplitsAndSubsets") {
      val subsetsToSplitAsIndices = subsetsToSplit.toArray
      withBroadcast(treeFeatures)(subsetsToSplitAsIndices) { br_splits =>
        val bestSplits = RegressionDecisionTree.findBestSplits(treeFeatures, br_splits, br_splitter)
        (bestSplits,
          RegressionDecisionTree.splitSubsets(treeFeatures, bestSplits, br_splits, br_splitter))
      }
    }
  }
}
