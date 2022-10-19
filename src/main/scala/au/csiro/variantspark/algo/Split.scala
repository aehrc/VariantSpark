package au.csiro.variantspark.algo

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
case class SplitInfo(splitPoint: Double, gini: Double, leftGini: Double, rightGini: Double)

/**
  * An aggregator for calculating split impurity for two sets of labels or values
  * indirectly referenced by their indexes.
  */
trait IndexedSplitAggregator {
  def left: ImpurityAggregator
  def right: ImpurityAggregator
  def reset() {
    left.reset()
    right.reset()
  }
  def update(agg: ImpurityAggregator) {
    left.add(agg)
    right.sub(agg)
  }

  /**
    *  Is this a valid split that is one that does not put
    *  all elements to one side
    */
  def hasProperSplit: Boolean = !left.isEmpty && !right.isEmpty

  /**
    * Get split impurity value
    */
  def getValue(outSplitImp: SplitImpurity): Double = {
    left.splitValue(right, outSplitImp)
  }
  def init(indexes: Array[Int]) {
    reset()
    indexes.foreach(i => init(i))
  }
  def init(index: Int)
  def update(index: Int)
}

/**
  * Split aggregator for classification. The indexes refer to nominal labels.
  */
class ClassificationSplitAggregator private (val labels: Array[Int],
    val left: ClassificationImpurityAggregator, val right: ClassificationImpurityAggregator)
    extends IndexedSplitAggregator {

  def initLabel(label: Int) {
    right.addLabel(label)
  }

  def updateLabel(label: Int) {
    left.addLabel(label)
    right.subLabel(label)
  }

  override def init(index: Int): Unit = initLabel(labels(index))

  override def update(index: Int): Unit = updateLabel(labels(index))
}

object ClassificationSplitAggregator {
  def apply(impurity: ClassificationImpurity, labels: Array[Int],
      nCategories: Int): ClassificationSplitAggregator =
    new ClassificationSplitAggregator(labels, impurity.createAggregator(nCategories),
      impurity.createAggregator(nCategories))
}

/**
  * Fast but memory intensive split aggregator keeping partial impurity statistics for
  * all the unique values of the feature (only makes senses with indexed features)
  */
class ConfusionAggregator private (val matrix: Array[ClassificationImpurityAggregator],
    val labels: Array[Int]) {

  def this(impurity: ClassificationImpurity, size: Int, nCategories: Int, labels: Array[Int]) {
    this(Array.fill(size)(impurity.createAggregator(nCategories)), labels)
  }

  /**
    * Reset the first nLevels of the matrix
    */
  def reset(nLevels: Int) {
    assert(nLevels <= matrix.length)
    matrix.iterator.take(nLevels).foreach(_.reset())
  }

  /**
    * Add a response at index yIndex for ordinal level
    */
  def updateAt(level: Int, yIndex: Int): Unit = matrix(level).addLabel(labels(yIndex))

  def apply(level: Int): ClassificationImpurityAggregator = matrix(level)
}

/**
  * The base interface for finding the best split in a set of indexed values.
  */
trait IndexedSplitter {
  def findSplit(splitIndices: Array[Int]): SplitInfo
}

/**
  * A helper trait for IndexedSplitter that select the actual implementation
  * base on the set of indexes themselves.
  */
trait SwitchingIndexedSplitter extends IndexedSplitter {
  def select(splitIndices: Array[Int]): IndexedSplitter
  override def findSplit(splitIndices: Array[Int]): SplitInfo =
    select(splitIndices).findSplit(splitIndices)
}

/**
  * Base interface for entities capable of producing indexes splitters
  */
trait SplitterProvider {
  def createSplitter(impCalc: IndexedSplitAggregator): IndexedSplitter
}

/**
  * Base interface for entities capable of producing fast but memory intensive confusion splitters
  */
trait FastSplitterProvider extends SplitterProvider {

  /**
    * The size of the required confusion aggregator
    */
  def confusionSize: Int
  def createSplitter(impCalc: IndexedSplitAggregator,
      confusionAgg: ConfusionAggregator): IndexedSplitter
}

/**
  * Base interface for the strategy for creating indexed splitters for a provider.
  */
trait IndexedSplitterFactory {
  def create(sf: SplitterProvider): IndexedSplitter
}

/**
  * Depending on weather the fast memory consuming splitter can be created
  * and the size of the current subset select either the fast memory consuming option
  * slower but memory efficient one
  * The way ranger does it is
  *  if (sampleSize/numOfUniqueValues < Q_THRESHOLD {
  *    useSlowAlgorithm()
  *  else {
  *   useFastAlgorithm() if (available I assume)
  *
  *  The value of Q_THRESHOLD is 0.02
  */
case class ThresholdIndexedSplitter(fastSplitter: IndexedSplitter, confusionSize: Int,
    defaultSplitter: IndexedSplitter,
    qThreshold: Double = ThresholdIndexesSplitter.DefaultQThreshold)
    extends SwitchingIndexedSplitter {

  override def select(splitIndices: Array[Int]): IndexedSplitter = {
    if (splitIndices.length.toDouble / confusionSize >= qThreshold) fastSplitter
    else defaultSplitter
  }
}

object ThresholdIndexesSplitter {
  val DefaultQThreshold: Double = 0.02
}

/**
  * The default implementation of the {{IndexedSplitterFactory}} for classification
  *
  */
class DefStatefulIndexedSplitterFactory(val impurity: ClassificationImpurity,
    val labels: Array[Int], val nCategories: Int, val maxConfusionSize: Int = 10,
    val qThreshold: Double = ThresholdIndexesSplitter.DefaultQThreshold)
    extends IndexedSplitterFactory {

  lazy val splitAggregator: ClassificationSplitAggregator =
    ClassificationSplitAggregator(impurity, labels, nCategories)
  lazy val confusionAgg: ConfusionAggregator =
    new ConfusionAggregator(impurity, maxConfusionSize, nCategories, labels)

  def create(sf: SplitterProvider): IndexedSplitter = {
    sf match {
      case fsf: FastSplitterProvider if fsf.confusionSize <= maxConfusionSize =>
        ThresholdIndexedSplitter(fsf.createSplitter(splitAggregator, confusionAgg),
          fsf.confusionSize, sf.createSplitter(splitAggregator), qThreshold)
      case _ => sf.createSplitter(splitAggregator)
    }
  }
}

/** REGRESSION SECTION */


/**
  * Split aggregator for classification. The indexes refer to nominal labels.
  */
class RegressionSplitterFactory(
    val impurity: RegressionImpurity, val labels: Array[Double])
    extends IndexedSplitterFactory {

  lazy val splitAggregator: RegressionSplitAggregator =
    RegressionSplitAggregator(impurity, labels)

  def create(sf: SplitterProvider): IndexedSplitter = {
    sf.createSplitter(splitAggregator)
  }
}

class RegressionSplitAggregator private (val labels: Array[Double],
    val left: RegressionImpurityAggregator, val right: RegressionImpurityAggregator)
    extends IndexedSplitAggregator {

  def initLabel(value: Double) {
    right.addValue(value)
  }

  def updateLabel(value: Double) {
    left.addValue(value)
    right.subValue(value)
  }

  override def init(index: Int): Unit = initLabel(labels(index))

  override def update(index: Int): Unit = updateLabel(labels(index))
}

object RegressionSplitAggregator {
  def apply(impurity: RegressionImpurity, labels: Array[Double]): RegressionSplitAggregator =
    new RegressionSplitAggregator(labels, impurity.createAggregator(),
      impurity.createAggregator())
}
