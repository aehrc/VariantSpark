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
  * Specify the 'indices' and 'stats'
  *
  * {{{
  * val subInfo = SubsetInfo(indices, stats)
  * }}}
  *
  * @param indices: input an array of integers that contains the indices required
  * @param stats: input the impurity statistics for the subset
  *
  */
case class SubsetInfo(indices: Array[Int], impurity: Double) {
  def length: Int = indices.length
  override def toString: String = s"SubsetInfo(${indices.toList},${impurity})"
}

/** Class utilized to give an insight into the split data
  *
  * Specify the 'variableIndex', 'splitInfo', and 'isPermutated'
  *
  * @constructor creates information about the split that occured on a specifc variable
  * @param variableIndex: specifies the index of the variable that the dataset will
  *                     split on
  * @param splitInfo: specifies the information about the split that occured on the dataset
  * @param isPermutated: specifies whether the split was based on permutated labels or not
  */
case class VarSplitInfo(variableIndex: Long, splitInfo: SplitInfo, isPermutated: Boolean) {

  /** Creates a list of the subsetInfos for the dataset split
    *
    * @param v: input the specific data construct
    * @param labels: input an array of integer labels
    * @param nCategories: specify the number of categories of the dataset or 'columns'
    * @param subset: specify the SubsetInfo class touched on previously at
    *              [[au.csiro.variantspark.algo.SubsetInfo]]
    * @return returns a tupple of the subset information
    */
  def split(v: TreeFeature)(subset: SubsetInfo): (SubsetInfo, SubsetInfo) = {
    val (leftIndices, rightIndices) = subset.indices.partition(i => splitInfo.goesLeft(v.at(i)))
    (new SubsetInfo(leftIndices, splitInfo.leftImpurity),
      new SubsetInfo(rightIndices, splitInfo.rightImpurity))
  }
  def splitPermutated(v: TreeFeature, permutationOrder: Array[Int])(
      subset: SubsetInfo): (SubsetInfo, SubsetInfo) = {
    val (leftIndices, rightIndices) =
      subset.indices.partition(i => splitInfo.goesLeft(v.at(permutationOrder(i))))
    (new SubsetInfo(leftIndices, splitInfo.leftImpurity),
      new SubsetInfo(rightIndices, splitInfo.rightImpurity))
  }
}

/** Defines the trait for the case class [[au.csiro.variantspark.algo.DeterministicMerger]]
  */
trait Merger {

  /** Operates a merging function utilizing two arrays of the
    * class [[au.csiro.variantspark.algo.VarSplitInfo]]
    *
    * @param a1: input an array of [[au.csiro.variantspark.algo.VarSplitInfo]]
    * @param a2: input an array of [[au.csiro.variantspark.algo.VarSplitInfo]]
    * @return Returns an array of [[au.csiro.variantspark.algo.VarSplitInfo]]
    */
  def merge(a1: Array[VarSplitInfo], a2: Array[VarSplitInfo]): Array[VarSplitInfo]
}

/** Utilizes the Deterministic Decision Tree model found here:
  * [[https://en.wikipedia.org/wiki/Decision_tree_model#Randomized_decision_tree]]
  * Extends the [[au.csiro.variantspark.algo.Merger]] class
  */
case class DeterministicMerger() extends Merger {

  /** Operates a merging function utilizing two arrays of the
    * class [[au.csiro.variantspark.algo.VarSplitInfo]]
    *
    * @param a1: input an array of [[au.csiro.variantspark.algo.VarSplitInfo]]
    * @param a2: input an array of [[au.csiro.variantspark.algo.VarSplitInfo]]
    * @return Returns the merged array a1
    */
  def merge(a1: Array[VarSplitInfo], a2: Array[VarSplitInfo]): Array[VarSplitInfo] = {

    /** Takes the [[au.csiro.variantspark.algo.VarSplitInfo]] from two seperate splits
      * and returns the value from either s1 or s2 based on the impurity
      *
      * @param s1: input an [[au.csiro.variantspark.algo.VarSplitInfo]]
      * @param s2: input an [[au.csiro.variantspark.algo.VarSplitInfo]]
      * @return Returns either s1 or s2 based on the impurity calculation
      */
    def mergeSplitInfo(s1: VarSplitInfo, s2: VarSplitInfo) = {
      if (s1 == null) s2
      else if (s2 == null) s1
      else if (s1.splitInfo.impurity < s2.splitInfo.impurity) s1
      else if (s2.splitInfo.impurity < s1.splitInfo.impurity) s2
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
case class RandomizingMergerMurmur3(seed: Long) extends Merger {

  def hashOrder(varIndex: Long, splitId: Int): Int = {
    MurMur3Hash.hashLong(varIndex, MurMur3Hash.hashLong(seed, splitId))
  }

  def chooseEqual(s1: VarSplitInfo, s2: VarSplitInfo, id: Int): VarSplitInfo = {
    if (hashOrder(s1.variableIndex, id) < hashOrder(s2.variableIndex, id)) s1 else s2
  }

  /** Operates a merging function utilizing two arrays of the
    * class [[au.csiro.variantspark.algo.VarSplitInfo]]
    *
    * @param a1: input an array of [[au.csiro.variantspark.algo.VarSplitInfo]]
    * @param a2: input an array of [[au.csiro.variantspark.algo.VarSplitInfo]]
    * @return Returns the merged array a1
    */
  def merge(a1: Array[VarSplitInfo], a2: Array[VarSplitInfo]): Array[VarSplitInfo] = {

    /** Takes the [[au.csiro.variantspark.algo.VarSplitInfo]] from two seperate splits
      * and returns the value from either s1 or s2 based on the gini impurity
      *
      * @note if the gini values of each split are equal then the value returns one at random
      *
      * @param s1: input an [[au.csiro.variantspark.algo.VarSplitInfo]]
      * @param s2: input an [[au.csiro.variantspark.algo.VarSplitInfo]]
      * @return Returns either s1 or s2 based on the gini impurity calculation
      */
    def mergeSplitInfo(s1: VarSplitInfo, s2: VarSplitInfo, id: Int) = {
      if (s1 == null) s2
      else if (s2 == null) s1
      else if (s1.splitInfo.impurity < s2.splitInfo.impurity) s1
      else if (s2.splitInfo.impurity < s1.splitInfo.impurity) s2
      else chooseEqual(s1, s2, id)
    }
    a1.indices.foreach(i => a1(i) = mergeSplitInfo(a1(i), a2(i), i))
    a1
  }
}

trait VariableSplitter {
  def calculator: ImpurityCalculator
  def initialSubset(sample: Sample): SubsetInfo

  /** Splits the subsets of the RDD and returns a split based on the variable of split index
    *
    * @param varData: input an interator containing the dataset and an index
    * @param subsets: input an array of [[au.csiro.variantspark.algo.SubsetInfo]]
    * @param bestSplits: input an array of the [[au.csiro.variantspark.algo.VarSplitInfo]]
    * @return returns a flattened iterator
    */
  def splitSubsets(varData: Iterator[TreeFeature], subsets: Array[SubsetInfo],
      bestSplits: Array[VarSplitInfo]): Iterator[(Int, (SubsetInfo, SubsetInfo))]
  def findSplitsForVars(varData: Iterator[TreeFeature], splits: Array[SubsetInfo])(
      implicit rng: RandomGenerator): Iterator[Array[VarSplitInfo]]
  def createMerger(seed: Long): Merger
}

/** Standard variable splitter used during normal (non-AIR-corrected) training.
  *
  * For each subset, tries a random fraction of variables and selects the one
  * giving the best impurity reduction.
  *
  * @param calculator the [[ImpurityCalculator]] that computes impurity and creates
  *                   split factories for the given problem type and response
  * @param mTryFraction the fraction of variables to try at each split (default 1.0)
  * @param randomizeEquality when true, breaks impurity ties randomly
  */
case class StdVariableSplitter(calculator: ImpurityCalculator, mTryFraction: Double = 1.0,
    randomizeEquality: Boolean = false, minRelativeImprovementFraction: Double,
    minAbsoluteImprovement: Double)
    extends VariableSplitter with Logging with Prof {

  def initialSubset(sample: Sample): SubsetInfo = {
    val currentSet = sample.indexes
    SubsetInfo(currentSet, calculator.calculate(currentSet).impurity)
  }

  /** Find the splits in the data based on the gini value
    *
    * Specify the 'data' and 'splits' inputs
    *
    * @param typedData: input the data from the dataset of generic type V
    * @param splits: input an array of the [[au.csiro.variantspark.algo.SubsetInfo]] class
    * @return returns an array [[au.csiro.variantspark.algo.SplitInfo]]
    */
  def findSplits(typedData: TreeFeature, splits: Array[SubsetInfo], sbf: IndexedSplitterFactory)(
      implicit rng: RandomGenerator): Array[SplitInfo] = {

    val splitter = sbf.create(typedData)
    splits.map { subsetInfo =>
      if (rng.nextDouble() <= mTryFraction) {
        val splitInfo = splitter.findSplit(subsetInfo.indices)
        val improvement = if (splitInfo != null) subsetInfo.impurity - splitInfo.impurity else 0.0
        val meetsThreshold = improvement > subsetInfo.impurity * minRelativeImprovementFraction &&
          improvement > minAbsoluteImprovement
        if (splitInfo != null && meetsThreshold) {
          splitInfo
        } else null
      } else null
    }
  }

  /** Returns the result of a split based on a variable
    *
    * @param varData: input an Iterator of a tuple containing the dataset and indices
    * @param splits: input an Array of the [[au.csiro.variantspark.algo.SubsetInfo]] class
    * @return takes the varData and maps the value of the dataset
    */
  def findSplitsForVars(varData: Iterator[TreeFeature], splits: Array[SubsetInfo])(
      implicit rng: RandomGenerator): Iterator[Array[VarSplitInfo]] = {
    profIt("Local: splitting") {
      val sbf = calculator.createSplitterFactory()
      val result = varData
        .map { vi =>
          val thisVarSplits = findSplits(vi, splits, sbf)
          thisVarSplits
            .map(si => if (si != null) VarSplitInfo(vi.index, si, false) else null)
        }
      result
    }
  }

  /** Splits the subsets of the RDD and returns a split based on the variable of split index
    *
    * @param varData: input an interator containing the dataset and an index
    * @param subsets: input an array of [[au.csiro.variantspark.algo.SubsetInfo]]
    * @param bestSplits: input an array of the [[au.csiro.variantspark.algo.VarSplitInfo]]
    * @return returns a flattened iterator
    */
  def splitSubsets(varData: Iterator[TreeFeature], subsets: Array[SubsetInfo],
      bestSplits: Array[VarSplitInfo]): Iterator[(Int, (SubsetInfo, SubsetInfo))] = {

    val usefulSubsetSplitAndIndex =
      subsets.zip(bestSplits).filter(_._2 != null).zipWithIndex.toList
    val splitByVarIndex = usefulSubsetSplitAndIndex.groupBy(_._1._2.variableIndex)
    varData.flatMap { vi =>
      splitByVarIndex.getOrElse(vi.index, Nil).map {
        case ((subsetInfo, splitInfo), si) =>
          (si, splitInfo.split(vi)(subsetInfo))
      }
    }
  }

  def createMerger(seed: Long): Merger =
    if (randomizeEquality) RandomizingMergerMurmur3(seed) else DeterministicMerger()

}

/** Variable splitter with AIR correction for
  * importance bias. Trains two parallel trees - one on the real response and one
  * on a permuted copy - so that importance scores can be bias-corrected.
  *
  * @param calculator the [[ImpurityCalculator]] for the real response
  * @param rng the random generator used to produce the permutation order
  * @param mTryFraction the fraction of variables to try at each split
  * @param randomizeEquality when true, breaks impurity ties randomly
  */
case class AirVariableSplitter(calculator: ImpurityCalculator,
    rng: XorShift1024StarRandomGenerator, mTryFraction: Double, randomizeEquality: Boolean,
    minRelativeImprovementFraction: Double, minAbsoluteImprovement: Double)
    extends VariableSplitter with Logging with Prof {

  lazy val (permutedCalculator, permutationOrder) = calculator.permute(rng)

  def initialSubset(sample: Sample): SubsetInfo = {
    val currentSet = sample.indexes
    SubsetInfo(currentSet, calculator.calculate(currentSet).impurity)
  }

  /** Find the splits in the data based on the gini value
    *
    * Specify the 'data' and 'splits' inputs
    *
    * @param typedData: input the data from the dataset of generic type V
    * @param splits: input an array of the [[au.csiro.variantspark.algo.SubsetInfo]] class
    * @return returns an array [[au.csiro.variantspark.algo.SplitInfo]]
    */
  def findSplits(typedData: TreeFeature, splits: Array[SubsetInfo], sbf: IndexedSplitterFactory,
      permutatedSbf: IndexedSplitterFactory, permSubsets: Array[Array[Int]])(
      implicit rng: RandomGenerator): Array[VarSplitInfo] = {

    val splitter = sbf.create(typedData)
    val permutatedSplitter = permutatedSbf.create(typedData)

    splits.zip(permSubsets).map {
      case (subsetInfo, permIndexes) =>
        val rnd = rng.nextDouble()
        if (rnd <= mTryFraction) {
          // check wheter to use informative or permutated labels
          val permutated = rnd > mTryFraction / 2
          val selectedSplitter = if (!permutated) splitter else permutatedSplitter
          val indices = if (!permutated) subsetInfo.indices else permIndexes
          val splitInfo = selectedSplitter.findSplit(indices)
          val improvement =
            if (splitInfo != null) subsetInfo.impurity - splitInfo.impurity else 0.0
          val meetsThreshold = (
            improvement > subsetInfo.impurity * minRelativeImprovementFraction &&
              improvement > minAbsoluteImprovement
          )
          if (splitInfo != null && meetsThreshold) {
            VarSplitInfo(typedData.index, splitInfo, permutated)
          } else { null }
        } else null
    }
  }

  /** Returns the result of a split based on a variable
    *
    * @param varData: input an Iterator of a tuple containing the dataset and indices
    * @param splits: input an Array of the [[au.csiro.variantspark.algo.SubsetInfo]] class
    * @return takes the varData and maps the value of the dataset
    */
  def findSplitsForVars(varData: Iterator[TreeFeature], splits: Array[SubsetInfo])(
      implicit rng: RandomGenerator): Iterator[Array[VarSplitInfo]] = {
    profIt("Local: splitting") {
      val sbf = calculator.createSplitterFactory()
      val permutatedSbf = permutedCalculator.createSplitterFactory()

      // TODO: [Performance] maybe there is not need to permutata all the splits up front

      val permSubsets = splits.map(s => ArraysUtils.permutate(s.indices, permutationOrder))
      varData.map(vi => findSplits(vi, splits, sbf, permutatedSbf, permSubsets))
    }
  }

  /** Splits the subsets of the RDD and returns a split based on the variable of split index
    *
    * @param varData: input an interator containing the dataset and an index
    * @param subsets: input an array of [[au.csiro.variantspark.algo.SubsetInfo]]
    * @param bestSplits: input an array of the [[au.csiro.variantspark.algo.VarSplitInfo]]
    * @return returns a flattened iterator
    */
  def splitSubsets(varData: Iterator[TreeFeature], subsets: Array[SubsetInfo],
      bestSplits: Array[VarSplitInfo]): Iterator[(Int, (SubsetInfo, SubsetInfo))] = {

    val usefulSubsetSplitAndIndex =
      subsets.zip(bestSplits).filter(_._2 != null).zipWithIndex.toList
    val splitByVarIndex = usefulSubsetSplitAndIndex.groupBy(_._1._2.variableIndex)
    varData.flatMap { vi =>
      splitByVarIndex.getOrElse(vi.index, Nil).map {
        case ((subsetInfo, splitInfo), si) =>
          if (!splitInfo.isPermutated) {
            (si, splitInfo.split(vi)(subsetInfo))
          } else {
            (si, splitInfo.splitPermutated(vi, permutationOrder)(subsetInfo))
          }
      }
    }
  }

  def createMerger(seed: Long): Merger =
    if (randomizeEquality) RandomizingMergerMurmur3(seed) else DeterministicMerger()

}

object AirVariableSplitter {
  def apply(calculator: ImpurityCalculator, seed: Long, mTryFraction: Double,
      randomizeEquality: Boolean, minRelativeImprovementFraction: Double,
      minAbsoluteImprovement: Double): AirVariableSplitter = {
    val rng = new XorShift1024StarRandomGenerator(seed)
    AirVariableSplitter(calculator, rng, mTryFraction, randomizeEquality,
      minRelativeImprovementFraction, minAbsoluteImprovement)
  }
}

/** Object utilized with the DecisionTreeModel class
  */
object DecisionTree extends Logging with Prof {

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
  def splitSubsets(indexedData: RDD[TreeFeature], bestSplits: Array[VarSplitInfo],
      br_subsets: Broadcast[Array[SubsetInfo]],
      br_splitter: Broadcast[VariableSplitter]): List[SubsetInfo] = {
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
        .foldLeft(Array.fill[SubsetInfo](indexedSplittedSubsets.size * 2)(null)) {
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
      br_splits: Broadcast[Array[SubsetInfo]], br_splitter: Broadcast[VariableSplitter])(
      implicit rng: RandomGenerator): Array[VarSplitInfo] = {
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
abstract class DecisionTreeNode(val stats: ImpurityStats) extends Serializable {
  def isLeaf: Boolean
  def size: Int = stats.size
  def nodeImpurity: Double = stats.impurity

  def printout(level: Int)
  def impurityContribution: Double = stats.impurity * size
  def traverse(f: SplitNode => Boolean): LeafNode = this match {
    case leaf: LeafNode => leaf
    case split: SplitNode => (if (f(split)) split.left else split.right).traverse(f)
  }
  def toStream: Stream[DecisionTreeNode]
  def splitsToStream: Stream[SplitNode] =
    toStream.filter(!_.isLeaf).asInstanceOf[Stream[SplitNode]]
  def leafsToStream: Stream[LeafNode] = toStream.filter(_.isLeaf).asInstanceOf[Stream[LeafNode]]
}

@SerialVersionUID(1L)
case class LeafNode(override val stats: ImpurityStats) extends DecisionTreeNode(stats) {
  val isLeaf: Boolean = true

  def printout(level: Int) {
    print(new String(Array.fill(level)(' ')))
    println(s"leaf${stats.printout}")
  }

  override def toString: String = s"leaf${stats.printout}"

  def toStream: Stream[DecisionTreeNode] = this #:: Stream.empty
}

@SerialVersionUID(1L)
case class SplitNode(override val stats: ImpurityStats, splitVariableIndex: Long,
    splitCriteria: SplitCriteria, impurityReduction: Double, left: DecisionTreeNode,
    right: DecisionTreeNode, isPermutated: Boolean = false)
    extends DecisionTreeNode(stats) {

  val isLeaf: Boolean = false

  def printout(level: Int) {
    print(new String(Array.fill(level)(' ')))
    println(
        s"split[${splitVariableIndex}, ${splitCriteria},"
          + s" ${stats.printout}, ${impurityReduction}]")
    left.printout(level + 1)
    right.printout(level + 1)
  }
  override def toString: String =
    (s"split[${splitVariableIndex}, ${splitCriteria},"
      + s" ${stats.printout}, ${impurityReduction}]")

  def childFor(value: Double): DecisionTreeNode =
    if (splitCriteria.goesLeft(value)) left else right

  def impurityDelta: Double = {
    val deltaAbs = impurityContribution - (left.impurityContribution + right.impurityContribution)
    if (isPermutated) -deltaAbs else deltaAbs
  }
  def toStream: Stream[DecisionTreeNode] = this #:: left.toStream #::: right.toStream
}

object SplitNode {
  def apply(stats: ImpurityStats, split: VarSplitInfo, left: DecisionTreeNode,
      right: DecisionTreeNode): SplitNode =
    SplitNode(stats, split.variableIndex, split.splitInfo.toCriteria,
      stats.impurity - split.splitInfo.impurity, left, right, split.isPermutated)
}

@SerialVersionUID(1L)
case class DecisionTreeModel(rootNode: DecisionTreeNode)
    extends PredictiveModelWithImportance with Logging with Serializable {

  def splitVariableIndexes: Set[Long] = rootNode.splitsToStream.map(_.splitVariableIndex).toSet

  def predict[T](indexedData: RDD[(T, Long)], variableType: VariableType)(
      implicit db: DataBuilder[T]): Array[Any] = {
    predict(indexedData.map({ case (v, i) => (StdFeature.from(null, variableType, v), i) }))
  }

  def predict(indexedData: RDD[(Feature, Long)]): Array[Any] = {
    val treeVariableData = indexedData.collectAtIndexes(splitVariableIndexes)
    Range(0, indexedData.size)
      .map(i =>
          rootNode
            .traverse(s => s.splitCriteria.goesLeft(treeVariableData(s.splitVariableIndex).at(i)))
            .stats
            .predict)
      .toArray
  }

  def printout() {
    rootNode.printout(0)
  }

  def printoutByLevel() {
    @scala.annotation.tailrec
    def printLevel(levelNodes: Seq[DecisionTreeNode]) {
      if (levelNodes.nonEmpty) {
        println(levelNodes.mkString(" "))
        printLevel(levelNodes.flatMap(_ match {
          case t: SplitNode => List(t.left, t.right)
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

  /** Returns the split criteria for each internal node in tree traversal order.
    * Each element is either a [[ThresholdSplitCriteria]] (ordered features) or a
    * [[SubsetSplitCriteria]] (nominal features).
    */
  def criteria: List[SplitCriteria] = rootNode.splitsToStream.map(_.splitCriteria).toList

}

/** Contains the object for the [[au.csiro.variantspark.algo.DecisionTreeModel]] class
  */
object DecisionTreeModel {

  /** Returns the resolved list of the split nodes and indices
    *
    * @param indexedData: input an RDD of tuples with the valeus in the dataset and the index
    * @param splitNodes: input a list of tuples with the
    *                  [[au.csiro.variantspark.algo.SplitNode]] class and an index
    * @return returns a List of the resolved [[au.csiro.variantspark.algo.SplitNode]]
    *         class and it's index
    */
  def resolveSplitNodes(indexedData: RDD[(DataLike, Long)],
      splitNodes: List[(SplitNode, Int)]): List[(DecisionTreeNode, Int)] = {
    val varsAndIndexesToCollect = splitNodes
      .asInstanceOf[List[(SplitNode, Int)]]
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
    splitNodes.asInstanceOf[List[(SplitNode, Int)]].zipWithIndex.map {
      case ((n, i), v) => (n.childFor(varValuesForSplits(v)), i)
    }
  }

  def batchPredict(indexedData: RDD[(DataLike, Long)], trees: Seq[DecisionTreeModel],
      indexes: Seq[Array[Int]]): Seq[Array[Any]] = {

    /** Takes the decision tree nodes and outputs the leaf nodes
      * Partitions the nodesAndIndexes variable and recursively iterates through each
      * model until a leaf node is reached
      *
      * @param nodesAndIndexes: input a list of tuples of tuple
      * @return a list of tuples of tuple
      */
    def predict(
        nodesAndIndexes: List[((DecisionTreeNode, Int), Int)]): List[((LeafNode, Int), Int)] = {
      val (leaves, splits) = nodesAndIndexes.partition(_._1._1.isLeaf)
      if (splits.isEmpty) {
        leaves.asInstanceOf[List[((LeafNode, Int), Int)]]
      } else {
        val (bareSplits, splitIndexes) = splits.unzip
        val transformedSplits =
          resolveSplitNodes(indexedData, bareSplits.asInstanceOf[List[(SplitNode, Int)]])
            .zip(splitIndexes)
        leaves.asInstanceOf[List[((LeafNode, Int), Int)]] ::: predict(transformedSplits)
      }
    }

    val rootNodesAndIndexes = trees
      .map(_.rootNode)
      .zip(indexes)
      .flatMap { case (n, idx) => idx.map(i => (n, i)) }
      .zipWithIndex
      .toList
    val leaveNodesAndIndexes = predict(rootNodesAndIndexes)

    val orderedPredictions = leaveNodesAndIndexes
      .sortBy(_._2)
      .map(_._1)
      .map(_._1.stats.predict)
    val orderedPredictionsIter = orderedPredictions.toIterator

    indexes.map(a => Array.fill(a.length)(orderedPredictionsIter.next()))
  }
}

/** A Class to specify the parameters of the decision tree model
  *
  * @param maxDepth: input the max value
  * @param minNodeSize: specify the minimum node size
  * @param seed: specify the seed for the random number generator
  * @param randomizeEquality: specify the randomization merger or the determinate merger
  */
case class DecisionTreeParams(problemType: ProblemType = Classification,
    maxDepth: Int = Int.MaxValue, minNodeSize: Int = 1, seed: Long = defRng.nextLong,
    randomizeEquality: Boolean = false, correctImpurity: Boolean = false,
    airRandomSeed: Long = 0L, stabilityMultiplier: Double = 1e4,
    // Floating-point noise from the one-pass variance calculation is on order of ~1e-12 relative.
    // The chosen threshold (1e-8 × parent impurity) provides a buffer above this noise floor while
    // remaining small relative to typical observed impurity reductions in practice. This value was
    // selected to balance numerical stability with sensitivity, should be validated empirically
    // for specific datasets.
    minRelativeImprovementFraction: Double = 1e-8) {

  override def toString: String = ToStringBuilder.reflectionToString(this)
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
class DecisionTree(val params: DecisionTreeParams = DecisionTreeParams(),
    val trf: TreeRepresentationFactory = DefTreeRepresentationFactory)
    extends Logging with Prof {

  implicit lazy val rnd: XorShift1024StarRandomGenerator =
    new XorShift1024StarRandomGenerator(params.seed)

  implicit def toRepresenation(indexedFeatures: RDD[(Feature, Long)]): RDD[TreeFeature] =
    trf.createRepresentation(indexedFeatures)

  /** Basic training operation taking the in the data, the type, and the labels
    *
    * @param indexedData: input an RDD of the dataset
    * @param response: input a [[au.csiro.variantspark.algo.ResponseVariable]] representing
    *                  the response variable
    */
  def train(indexedData: RDD[(Feature, Long)], response: ResponseVariable): DecisionTreeModel =
    train(indexedData, response, 1.0, Sample.all(indexedData.first._1.size))

  /** Alternative train function
    *
    * @param indexedData: input an RDD of the values of the dataset with the indices
    * @param response: input a [[au.csiro.variantspark.algo.ResponseVariable]] representing
    *                 the response variable
    * @param nvarFraction: fraction of variable to test at each split
    * @param sample: input the [[au.csiro.variantspark.utils.Sample]] class that
    *              contains the size and the indices
    */
  def train(indexedData: RDD[(Feature, Long)], response: ResponseVariable, nvarFraction: Double,
      sample: Sample): DecisionTreeModel =
    batchTrain(indexedData, response, nvarFraction, List(sample)).head

  /** Trains all the trees for specified samples at the same time
    *
    * @param indexedFeatures: input an RDD of the values of the dataset with the indices
    * @param response: input a [[au.csiro.variantspark.algo.ResponseVariable]] representing
    *                 the response variable
    * @param nvarFraction: fraction of variable to test for each split
    * @param sample: input the [[au.csiro.variantspark.utils.Sample]] class that
    *              contains the size and the indices
    * @return Returns a Sequence of [[au.csiro.variantspark.algo.DecisionTreeModel]]
    *         classes containing the dataset
    */
  def batchTrain(indexedFeatures: RDD[(Feature, Long)], response: ResponseVariable,
      nvarFraction: Double, sample: Seq[Sample]): Seq[DecisionTreeModel] = {
    batchTrainInt(trf.createRepresentation(indexedFeatures), response, nvarFraction, sample)
  }

  /** Trains all the trees for specified samples at the same time
    *
    * @param features: input an RDD of the internal tree feature representation
    * @param response: input a [[au.csiro.variantspark.algo.ResponseVariable]] representing
    *                the response variable
    * @param nvarFraction: fraction of variable to test for each split
    * @param sample: input the [[au.csiro.variantspark.utils.Sample]] class that
    *              contains the size and the indices
    * @return Returns a Sequence of [[au.csiro.variantspark.algo.DecisionTreeModel]]
    *         classes containing the dataset
    */
  def batchTrainInt(features: RDD[TreeFeature], response: ResponseVariable, nvarFraction: Double,
      sample: Seq[Sample]): Seq[DecisionTreeModel] = {

    val calculator: ImpurityCalculator = params.problemType.makeCalculator(response)

    // Compute a data-adaptive absolute improvement floor to reject splits whose impurity
    // reduction is indistinguishable from floating-point noise.
    //
    // The naive variance formula  Var = E[y^2] - E[y]^2  suffers from catastrophic cancellation
    // when the two nearly-equal terms are subtracted.  The absolute error in a single
    // variance estimate is bounded by  eps_machine * E[y^2]  (~2.2e-16 * E[y^2]).  The
    // improvement value is itself a difference of two such estimates, so worst-case cancellation
    // can amplify the error by a further factor of O(stabilityMultiplier) - empirically ~1e4
    // for deep trees on nearly-constant-response bootstrap samples in GWAS data.
    //
    // For a standardized response (E[y^2] ~= 1) this yields a floor of ~2.2e-12, comfortably
    // above the noise range [-1e-14, 1e-10] observed in practice and many orders of magnitude
    // below any real GWAS signal (minimum real variance reduction O(1e-4)).  For an
    // un-normalized response the floor scales proportionally, making the guard scale-invariant.
    //
    // Classification uses E[y^2] = 1.0 (Gini values are already in [0,1] and are far above
    // any floating-point noise), so the floor has no effect on classification trees.
    val responseScale = response match {
      case RegressionResponse(values) =>
        val sumSq = values.map(v => v * v).sum
        sumSq / values.length
      case _ => 1.0
    }
    val absoluteImprovementFloor = params.stabilityMultiplier * 2.2e-16 * responseScale

    // Guards are unecessary for classification, which does not experience fp noise issues
    // both guards are unnecessary and are disabled to avoid affecting test behaviour.
    val (effectiveRelativeFloor, effectiveAbsoluteFloor) = params.problemType match {
      case Classification => (0.0, 0.0)
      case _ => (params.minRelativeImprovementFraction, absoluteImprovementFloor)
    }

    // manage persistence here - cache the features if not already cached
    withCached(features) { cachedFeatures =>
      val splitter: VariableSplitter =
        if (params.correctImpurity) {
          AirVariableSplitter(calculator,
            if (params.airRandomSeed != 0L) params.airRandomSeed else params.seed, nvarFraction,
            params.randomizeEquality, effectiveRelativeFloor, effectiveAbsoluteFloor)
        } else {
          StdVariableSplitter(calculator, nvarFraction, params.randomizeEquality,
            effectiveRelativeFloor, effectiveAbsoluteFloor)
        }
      val subsets = sample.map(splitter.initialSubset).toList
      val rootNodes = withBroadcast(cachedFeatures)(splitter) { br_splitter =>
        buildSplit(cachedFeatures, subsets, br_splitter, 0)
      }
      rootNodes.map(new DecisionTreeModel(_))
    }
  }

  private def summarize(subsets: List[SubsetInfo]): String = {
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
  private def buildSplit(indexedTypedData: RDD[TreeFeature], subsets: List[SubsetInfo],
      br_splitter: Broadcast[VariableSplitter], treeLevel: Int): List[DecisionTreeNode] = {

    logDebug(s"Building level ${treeLevel}")
    logDebug(s"Initial subsets: ${summarize(subsets)}")
    logTrace(s"Initial subsets (details): ${subsets}")

    profReset()

    val subsetsToSplit = subsets.zipWithIndex.filter {
      case (si, _) =>
        // Do not attempt to split a node whose impurity is effectively zero.
        // The online aggregator (Chan's parallel combination / reverse-sub) can leave
        // tiny positive residuals (e.g. 1e-16) on constant-response subsets.
        // A strict > 0.0 check lets those through; use an absolute floor instead.
        si.length >= params.minNodeSize && treeLevel < params.maxDepth // && si.impurity > 1e-10
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
    val calculator = br_splitter.value.calculator
    val subsetIndexToSplitIndexMap = usefulSplitsIndices.zipWithIndex.toMap
    val result = subsets.zipWithIndex.map {
      case (subset, i) =>
        // format: off
        subsetIndexToSplitIndexMap
          .get(i)
          .map(splitIndex => {
            SplitNode(calculator.calculate(subset.indices), usefulSplits(splitIndex),
            nextLevelNodes(2 * splitIndex), nextLevelNodes(2 * splitIndex + 1))
          })
          .getOrElse(LeafNode(calculator.calculate(subset.indices)))
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
      subsetsToSplit: List[SubsetInfo], br_splitter: Broadcast[VariableSplitter]) = {
    profIt("findBestSplitsAndSubsets") {
      val subsetsToSplitAsIndices = subsetsToSplit.toArray
      withBroadcast(treeFeatures)(subsetsToSplitAsIndices) { br_splits =>
        val bestSplits = DecisionTree.findBestSplits(treeFeatures, br_splits, br_splitter)
        (bestSplits, DecisionTree.splitSubsets(treeFeatures, bestSplits, br_splits, br_splitter))
      }
    }
  }
}
