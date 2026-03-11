package au.csiro.variantspark.algo

/** The lean, prediction-time representation of a split decision stored in a tree node.
  * Encodes only what is needed to route a sample left or right at inference time.
  */
@SerialVersionUID(1L)
sealed trait SplitCriteria extends Serializable {
  def goesLeft(value: Double): Boolean
}

/** A split criteria for ordered (continuous, discrete, or ordinal) features.
  * Routes a sample left if its value is less than or equal to the split point.
  *
  * @param splitPoint the threshold value at which the split occurs
  */
case class ThresholdSplitCriteria(splitPoint: Double) extends SplitCriteria {
  def goesLeft(value: Double): Boolean = value <= splitPoint
}

/** A split criteria for nominal features.
  * Routes a sample left if its integer level is a member of the subset encoded
  * by the bitmask (i.e. bit {{level}} is set).
  *
  * @param mask a Long bitmask where bit i set means level i goes left
  */
case class SubsetSplitCriteria(mask: Long) extends SplitCriteria {
  def goesLeft(value: Double): Boolean = (mask & (1L << value.toInt)) != 0
}

/** Build-time information about a split, combining the prediction criteria with
  * the impurity statistics needed during tree construction.
  * Only lives during training; the lean [[SplitCriteria]] is extracted via
  * {{toCriteria}} and stored in the persisted [[SplitNode]].
  *
  * @param gini the weighted gini impurity of the split
  * @param leftGini the gini impurity of the left child
  * @param rightGini the gini impurity of the right child
  */
sealed trait SplitInfo {
  def gini: Double
  def leftGini: Double
  def rightGini: Double
  def goesLeft(value: Double): Boolean
  def toCriteria: SplitCriteria
}

/** [[SplitInfo]] for ordered (continuous, discrete, or ordinal) features.
  *
  * @param splitPoint the threshold value at which the split occurs
  * @param gini the weighted gini impurity of the split
  * @param leftGini the gini impurity of the left child
  * @param rightGini the gini impurity of the right child
  */
case class ThresholdSplitInfo(splitPoint: Double, gini: Double, leftGini: Double,
    rightGini: Double)
    extends SplitInfo {
  def goesLeft(value: Double): Boolean = value <= splitPoint
  def toCriteria: SplitCriteria = ThresholdSplitCriteria(splitPoint)
}

/** [[SplitInfo]] for nominal features.
  *
  * @param mask a Long bitmask where bit i set means level i goes left
  * @param gini the weighted gini impurity of the split
  * @param leftGini the gini impurity of the left child
  * @param rightGini the gini impurity of the right child
  */
case class SubsetSplitInfo(mask: Long, gini: Double, leftGini: Double, rightGini: Double)
    extends SplitInfo {
  def goesLeft(value: Double): Boolean = (mask & (1L << value.toInt)) != 0
  def toCriteria: SplitCriteria = SubsetSplitCriteria(mask)
}

/**
  * An aggregator for calculating split impurity for two sets of labels or values
  * indireclty referenced by theid indexes.
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
class ClassificationSplitAggregator private (val labels: Array[Int], val nCategories: Int,
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
  def apply(impurity: ClassficationImpurity, labels: Array[Int],
      nCategories: Int): ClassificationSplitAggregator =
    new ClassificationSplitAggregator(labels, nCategories, impurity.createAggregator(nCategories),
      impurity.createAggregator(nCategories))
}

/**
  * Fast but memory intensive split aggregator keeping partial impurity statistics for
  * all the unique values of the feature (only makes senses with indexed features)
  */
class ConfusionAggregator private (val matrix: Array[ClassificationImpurityAggregator],
    val labels: Array[Int]) {

  def this(impurity: ClassficationImpurity, size: Int, nCategories: Int, labels: Array[Int]) {
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
  * A helper trait for IndexedSplitter that select the actual implementaiton
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
    * The size of the required confusino aggregator
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
  * slower but memory efficien one
  * The way ranger does it is
  *  if (sampleSize/numOfUniqueValues < Q_THRESHOLD {
  *    useSlowAlgorirm()
  *  else {
  *   useFastAltorithm() if (available I assume)
  *
  *  The value of Q_THRESHOLD is 0.02
  */
case class ThresholdIndexedSplitter(fastSplitter: IndexedSplitter, confusionSize: Int,
    defaultSplitter: IndexedSplitter,
    qThreshold: Double = ThresholdIndexesSplitter.DefaultQThredhold)
    extends SwitchingIndexedSplitter {

  override def select(splitIndices: Array[Int]): IndexedSplitter = {
    if (splitIndices.length.toDouble / confusionSize >= qThreshold) fastSplitter
    else defaultSplitter
  }
}

object ThresholdIndexesSplitter {
  val DefaultQThredhold: Double = 0.02
}

/**
  * The default implementation of the {{IndexedSplitterFactory}} for classification
  *
  */
class DefStatefullIndexedSpliterFactory(val impurity: ClassficationImpurity,
    val labels: Array[Int], val nCategories: Int, val maxConfusionSize: Int = 10,
    val qThreshold: Double = ThresholdIndexesSplitter.DefaultQThredhold)
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
