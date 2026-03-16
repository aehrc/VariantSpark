package au.csiro.variantspark.algo

import au.csiro.pbdava.ssparkle.common.utils.FastUtilConversions._
import au.csiro.pbdava.ssparkle.common.utils.Logging
import au.csiro.pbdava.ssparkle.common.utils.Timed._
import au.csiro.variantspark.data.Feature
import au.csiro.variantspark.metrics.Metrics
import au.csiro.variantspark.utils.IndexedRDDFunction._
import au.csiro.variantspark.utils.{Sample, defRng}
import it.unimi.dsi.fastutil.longs.{Long2DoubleOpenHashMap, Long2LongOpenHashMap}
import it.unimi.dsi.util.XorShift1024StarRandomGenerator
import org.apache.commons.lang3.builder.ToStringBuilder
import org.apache.spark.rdd.RDD

/** Allows for normalization(scaling)of the input map values
  */
trait VarImportanceNormalizer {
  def normalize(varImportance: Map[Long, Double]): Map[Long, Double]
}

/** Defines normalization variable conditionally
  */
case object RawVarImportanceNormalizer extends VarImportanceNormalizer {
  override def normalize(varImportance: Map[Long, Double]): Map[Long, Double] = varImportance
}

/** Implements normalization variable scaling
  */
class StandardImportanceNormalizer(val scale: Double) extends VarImportanceNormalizer {
  override def normalize(varImportance: Map[Long, Double]): Map[Long, Double] = {
    val total = varImportance.values.sum * scale
    varImportance.mapValues(_ / total)
  }
}

/** Defines two different scaling values conditionally - 100% and 1%
  */
case object To100ImportanceNormalizer extends StandardImportanceNormalizer(100.0)
case object ToOneImportanceNormalizer extends StandardImportanceNormalizer(1.0)

/** Implements random forest members conditionally
  * @param predictor the predictor model
  * @param oobIndexes an array of out-of-bag index values
  */
@SerialVersionUID(2L)
case class RandomForestMember(predictor: PredictiveModelWithImportance,
    oobIndexes: Array[Int] = null, oobPred: Array[Any] = null) {}

/** Implements random forest models conditionally
  * @param members the RF members
  * @param labelCount the label count
  * @param oobErrors the out-of-bag errors
  */
@SerialVersionUID(2L)
case class RandomForestModel(members: List[RandomForestMember],
    aggregatorFactory: PredictionAggregatorFactory, oobErrors: List[Double] = List.empty,
    params: RandomForestParams = null) {

  def oobError: Double = oobErrors.last

  def printout() {
    trees.zipWithIndex.foreach {
      case (tree, index) =>
        println(s"Tree: ${index}")
        tree.printout()
    }
  }

  def trees: List[PredictiveModelWithImportance] = members.map(_.predictor)

  def normalizedVariableImportance(
      norm: VarImportanceNormalizer = To100ImportanceNormalizer): Map[Long, Double] =
    norm.normalize(variableImportance)

  /** Sets the variable importance by averaging the importance of each variable over all trees
    *  if a variable is not used in a tree it's importance for this tree is assumed to be 0
    */
  def variableImportance: Map[Long, Double] = {

    trees
      .map(_.variableImportanceAsFastMap)
      .foldLeft(new Long2DoubleOpenHashMap())(_.addAll(_))
      .asScala
      .mapValues(_ / size)
  }

  /**
    * Computes the number of time each of the variables appears as the splitting variable
    * in the forest.
    * @return map variableIndex -> variableSplitCount
    */
  def variableSplitCount: Map[Long, Long] = {
    trees
      .map(_.variableSplitCountAsFastMap)
      .foldLeft(new Long2LongOpenHashMap())(_.addAll(_))
      .asScala
  }

  def size: Int = members.size

  def predict(indexedData: RDD[(Feature, Long)]): Array[Any] =
    predict(indexedData, indexedData.size)

  def predict(indexedData: RDD[(Feature, Long)], nSamples: Int): Array[Any] = {
    trees.iterator
      .map(_.predict(indexedData))
      .foldLeft(aggregatorFactory.create(nSamples))(_.addPredictions(_))
      .predictions
  }

  def nCategories: Int = aggregatorFactory match {
    case VotingAggregatorFactory(n) => n
    case _ =>
      throw new UnsupportedOperationException(
          "nCategories is only available for classification models")
  }

  def predictProb(indexedData: RDD[(Feature, Long)]): Array[Array[Double]] =
    predictProb(indexedData, indexedData.size)

  def predictProb(indexedData: RDD[(Feature, Long)], nSamples: Int): Array[Array[Double]] = {
    val agg = trees
      .map(_.predict(indexedData))
      .foldLeft(aggregatorFactory.create(nSamples))(_.addPredictions(_))
    agg match {
      case v: VotingAggregator => v.classProbabilities
      case _ =>
        throw new UnsupportedOperationException(
            "predictProb is only supported for classification models")
    }
  }
}

/** Implements random forest params conditionally
  * @param oob the out-of-bag value
  * @param nTryFraction the n-try fraction value
  * @param bootstrap the bootstrap value
  * @param subsample the subsample value
  * @param seed the seed value
  * @param maxDepth the maxDepth value
  * @param minNodeSize the minNodeSize value
  */
case class RandomForestParams(problemType: ProblemType = Classification, oob: Boolean = true,
    nTryFraction: Double = Double.NaN, bootstrap: Boolean = true, subsample: Double = Double.NaN,
    randomizeEquality: Boolean = true, seed: Long = defRng.nextLong, maxDepth: Int = Int.MaxValue,
    minNodeSize: Int = 1, correctImpurity: Boolean = false, airRandomSeed: Long = 0L) {
  def resolveDefaults(nSamples: Int, nVariables: Int): RandomForestParams = {
    RandomForestParams(problemType = problemType, oob = oob,
      nTryFraction =
        if (!nTryFraction.isNaN) nTryFraction
        else if (problemType == Classification) Math.sqrt(nVariables.toDouble) / nVariables
        else 0.33,
      bootstrap = bootstrap,
      subsample = if (!subsample.isNaN) subsample else if (bootstrap) 1.0 else 0.666,
      randomizeEquality = randomizeEquality, seed = seed, maxDepth = maxDepth,
      minNodeSize = minNodeSize, correctImpurity = correctImpurity, airRandomSeed = airRandomSeed)
  }
  def toDecisionTreeParams(seed: Long): DecisionTreeParams = {
    DecisionTreeParams(problemType = problemType, seed = seed,
      randomizeEquality = randomizeEquality, maxDepth = maxDepth, minNodeSize = minNodeSize,
      correctImpurity = correctImpurity, airRandomSeed = airRandomSeed)
  }
  override def toString: String = ToStringBuilder.reflectionToString(this)
}

object RandomForestParams {
  def fromOptions(problemType: Option[ProblemType] = None, oob: Option[Boolean] = None,
      mTryFraction: Option[Double] = None, bootstrap: Option[Boolean] = None,
      subsample: Option[Double] = None, seed: Option[Long] = None, maxDepth: Option[Int] = None,
      minNodeSize: Option[Int] = None, correctImpurity: Option[Boolean] = None,
      airRandomSeed: Option[Long] = None): RandomForestParams =
    RandomForestParams(problemType.getOrElse(Classification), oob.getOrElse(true),
      mTryFraction.getOrElse(Double.NaN), bootstrap.getOrElse(true),
      subsample.getOrElse(Double.NaN), true, seed.getOrElse(defRng.nextLong),
      maxDepth.getOrElse(Int.MaxValue), minNodeSize.getOrElse(1),
      correctImpurity.getOrElse(false), airRandomSeed.getOrElse(0L))
}

trait RandomForestCallback {
  def onParamsResolved(actualParams: RandomForestParams) {}
  def onTreeComplete(nTrees: Int, oobError: Double, elapsedTimeMs: Long) {}
}

// TODO (Design): Avoid using type cast change design
trait BatchTreeModel {
  def batchTrain(indexedData: RDD[TreeFeature], response: ResponseVariable, nTryFraction: Double,
      samples: Seq[Sample]): Seq[PredictiveModelWithImportance]
  def batchPredict(indexedData: RDD[TreeFeature], models: Seq[PredictiveModelWithImportance],
      indexes: Seq[Array[Int]]): Seq[Array[Any]]
}

object RandomForest {
  type ModelBuilderFactory = DecisionTreeParams => BatchTreeModel
  val defaultBatchSize: Int = 10

  def wideDecisionTreeBuilder(params: DecisionTreeParams): BatchTreeModel = {
    val decisionTree = new DecisionTree(params)
    new BatchTreeModel() {
      override def batchTrain(indexedData: RDD[TreeFeature], response: ResponseVariable,
          nTryFraction: Double, samples: Seq[Sample]): Seq[PredictiveModelWithImportance] =
        decisionTree.batchTrainInt(indexedData, response, nTryFraction, samples)
      override def batchPredict(indexedData: RDD[TreeFeature],
          models: Seq[PredictiveModelWithImportance], indexes: Seq[Array[Int]]): Seq[Array[Any]] =
        DecisionTreeModel.batchPredict(indexedData.map(tf => (tf, tf.index)),
          models.asInstanceOf[Seq[DecisionTreeModel]], indexes)
    }
  }
}

/** Implements random forest
  * @param params the RF params
  * @param modelBuilderFactory the type of model, i.e. 'wide decision tree builder'
  */
class RandomForest(params: RandomForestParams = RandomForestParams(),
    modelBuilderFactory: RandomForest.ModelBuilderFactory = RandomForest.wideDecisionTreeBuilder,
    trf: TreeRepresentationFactory = DefTreeRepresentationFactory)
    extends Logging {

  // TODO (Design):make this class keep random state (could be externalised to implicit random)
  implicit lazy val rng: XorShift1024StarRandomGenerator =
    new XorShift1024StarRandomGenerator(params.seed)
  def batchTrain(indexedData: RDD[(Feature, Long)], response: ResponseVariable, nTrees: Int,
      nBatchSize: Int = RandomForest.defaultBatchSize): RandomForestModel = {
    val treeFeatures: RDD[TreeFeature] = trf.createRepresentation(indexedData)
    batchTrainTyped(treeFeatures, response, nTrees, nBatchSize)
  }

  // TODO (Design): Make a param rather then an extra method
  // TODO (Func): Add OOB Calculation
  def batchTrainTyped(treeFeatures: RDD[TreeFeature], response: ResponseVariable, nTrees: Int,
      nBatchSize: Int)(implicit callback: RandomForestCallback = null): RandomForestModel = {

    require(nBatchSize > 0)
    require(nTrees > 0)
    val nSamples = response.length
    val nVariables = treeFeatures.count().toInt

    logDebug(s"Data:  nSamples:${nSamples}, nVariables: ${nVariables}")

    val actualParams = params.resolveDefaults(nSamples, nVariables)

    val calculator = actualParams.problemType.makeCalculator(response)
    val aggregatorFactory = calculator.createPredictionAggregatorFactory()

    Option(callback).foreach(_.onParamsResolved(actualParams))
    logDebug(s"Parameters: ${actualParams}")
    logDebug(s"Batch Training: ${nTrees} with batch size: ${nBatchSize}")

    // TODO: Custom OOB aggregation
    val oobAggregator: Option[PredictionAggregator] =
      if (actualParams.oob) Some(aggregatorFactory.create(nSamples)) else None

    val builder = modelBuilderFactory(actualParams.toDecisionTreeParams(rng.nextLong))
    val allSamples = Stream
      .fill(nTrees)(Sample.fraction(nSamples, actualParams.subsample, actualParams.bootstrap))

    val (allTrees, errors) = allSamples
      .sliding(nBatchSize, nBatchSize)
      .flatMap { samplesStream =>
        time {

          val samples = samplesStream.toList
          val predictors =
            builder.batchTrain(treeFeatures, response, actualParams.nTryFraction, samples)
          val members = if (actualParams.oob) {

            val oobIndexes = samples.map(_.distinctIndexesOut.toArray)
            val oobPredictions = builder.batchPredict(treeFeatures, predictors, oobIndexes)
            predictors.zip(oobIndexes.zip(oobPredictions)).map {
              case (t, (i, p)) => RandomForestMember(t, i, p)
            }

          } else predictors.map(RandomForestMember(_))

          val oobError = oobAggregator
            .map { agg =>
              members.map { m =>
                agg.addPredictions(m.oobPred, m.oobIndexes)
                response match {
                  case ClassificationResponse(labels) =>
                    Metrics.classificationError(labels, agg.predictions.map(_.asInstanceOf[Int]))
                  case RegressionResponse(values) =>
                    Metrics.rootMeanSquaredError(values,
                      agg.predictions.map(_.asInstanceOf[Double]))
                }
              }
            }
            .getOrElse(List.fill(predictors.size)(Double.NaN))
          members.zip(oobError)
        }.withResultAndTime {
          case (treesAndErrors, elapsedTime) =>
            logDebug(s"Trees: ${treesAndErrors.size} >> oobError: ${treesAndErrors.last._2}, "
                + s"time: ${elapsedTime}")
            Option(callback).foreach(_.onTreeComplete(treesAndErrors.size, treesAndErrors.last._2,
                elapsedTime))
        }.result
      }
      .toList
      .unzip

    RandomForestModel(allTrees, aggregatorFactory, errors, actualParams)
  }
}
