package au.csiro.variantspark.algo

import au.csiro.pbdava.ssparkle.common.utils.FastUtilConversions._
import au.csiro.pbdava.ssparkle.common.utils.Logging
import au.csiro.pbdava.ssparkle.common.utils.Timed._
import au.csiro.variantspark.data.VariableType
import au.csiro.variantspark.metrics.Metrics
import au.csiro.variantspark.utils.IndexedRDDFunction._
import au.csiro.variantspark.utils.{Sample, defRng}
import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap
import it.unimi.dsi.util.XorShift1024StarRandomGenerator
import org.apache.commons.lang3.builder.ToStringBuilder
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag
import au.csiro.variantspark.data.Feature
import au.csiro.variantspark.data.FeatureBuilder

/** Allows for normalization(scaling)of the input map values
  */
trait VarImportanceNormalizer {
  def normalize(varImportance:Map[Long,Double]):Map[Long, Double]
}

/** Defines normalization variable conditionally
  */
case object RawVarImportanceNormalizer extends VarImportanceNormalizer {
  override def normalize(varImportance:Map[Long,Double]):Map[Long, Double] = varImportance
}

/** Implements normalization variable scaling
  */
class StandardImportanceNormalizer(val scale:Double) extends VarImportanceNormalizer {
  override def normalize(varImportance:Map[Long,Double]):Map[Long, Double] = {
    val total = varImportance.values.sum  * scale
    varImportance.mapValues(_/total)
  }
}

/** Defines two different scaling values conditionally - 100% and 1%
  */
case object To100ImportanceNormalizer extends StandardImportanceNormalizer(100.0) 
case object ToOneImportanceNormalizer extends StandardImportanceNormalizer(1.0)

/** Implements voting aggregator conditionally
  * @param nLabels the number of labels
  * @param nSamples the number of samples
  */
case class VotingAggregator(val nLabels:Int, val nSamples:Int) {
  lazy val votes = Array.fill(nSamples)(Array.fill(nLabels)(0))

  /** Adds a vote with predictions and indexes
    * @param predictions the number of predictions
    * @param indexes the number of indexes
    */
  def addVote(predictions:Array[Int], indexes:Iterable[Int]) {
    require(predictions.length <= nSamples, "Valid number of samples")
    predictions.zip(indexes).foreach{ case(v, i) => votes(i)(v) += 1}
  }

  /** Adds a vote with predictions
    * @param predictions the number of predictions
    */
  def addVote(predictions:Array[Int]):VotingAggregator = {
    require(predictions.length == nSamples, "Full prediction range")
    predictions.zipWithIndex.foreach{ case(v, i) => votes(i)(v) += 1}
    this
  }

  /** Maps votes to predictions
    *
    */
  def predictions = votes.map(_.zipWithIndex.maxBy(_._1)._2)
}

/** Implements random forest members conditionally
  * @param predictor the predictor model
  * @param oobIndexes an array of out-of-bag index values
  */
@SerialVersionUID(2l)
case class RandomForestMember(val predictor:PredictiveModelWithImportance,
                                 val oobIndexes:Array[Int] = null, val oobPred:Array[Int] = null) {
}

/** Implements random forest models conditionally
  * @param members the RF members
  * @param labelCount the label count
  * @param oobErrors the out-of-bag errors
  */
@SerialVersionUID(2l)
case class RandomForestModel(val members: List[RandomForestMember], val labelCount:Int, val oobErrors:
  List[Double] = List.empty, val params:RandomForestParams = null) {

  def size = members.size
  def trees = members.map(_.predictor)

  def oobError:Double = oobErrors.last

  def printout() {
    trees.zipWithIndex.foreach {
      case (tree, index) =>
        println(s"Tree: ${index}")
        tree.printout()
    }
  }

  def normalizedVariableImportance(norm:VarImportanceNormalizer = To100ImportanceNormalizer):
    Map[Long, Double] = norm.normalize(variableImportance)

  /** Sets the variable importance by averaging the importance of each variable over all trees
    *  if a variable is not used in a tree it's importance for this tree is assumed to be 0
    */
  def variableImportance: Map[Long, Double] = {

    trees.map(_.variableImportanceAsFastMap).foldLeft(new Long2DoubleOpenHashMap())(_.addAll(_))
      .asScala.mapValues(_/size)
  }

  def predict(indexedData: RDD[(Feature, Long)]): Array[Int] = predict(indexedData, indexedData.size)
  
  def predict(indexedData: RDD[(Feature, Long)], nSamples:Int): Array[Int] = {
     trees.map(_.predict(indexedData))
       .foldLeft(VotingAggregator(labelCount, nSamples))(_.addVote(_)).predictions
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
case class RandomForestParams(
    oob:Boolean = true,
    nTryFraction:Double =  Double.NaN,
    bootstrap:Boolean = true,
    subsample:Double = Double.NaN,
    randomizeEquality:Boolean = true,
    seed:Long =  defRng.nextLong,
    maxDepth:Int = Int.MaxValue,
    minNodeSize:Int = 1, 
    correctImpurity:Boolean = false, 
    airRandomSeed:Long = 0L
) {
  def resolveDefaults(nSamples:Int, nVariables:Int):RandomForestParams = {
    RandomForestParams(
        oob = oob,
        nTryFraction = if (!nTryFraction.isNaN) nTryFraction else Math.sqrt(nVariables.toDouble)/nVariables,
        bootstrap = bootstrap,
        subsample = if (!subsample.isNaN) subsample else if (bootstrap) 1.0 else 0.666,
        randomizeEquality  = randomizeEquality,
        seed = seed,
        maxDepth = maxDepth,
        minNodeSize = minNodeSize, 
        correctImpurity = correctImpurity, 
        airRandomSeed = airRandomSeed
    )
  }
  def toDecisionTreeParams(seed:Long): DecisionTreeParams = { 
    DecisionTreeParams(
        seed = seed, 
        randomizeEquality = randomizeEquality, 
        maxDepth = maxDepth, 
        minNodeSize = minNodeSize, 
        correctImpurity = correctImpurity, 
        airRandomSeed = airRandomSeed
    )
  }
  override def toString = ToStringBuilder.reflectionToString(this)
}

object RandomForestParams {
  def fromOptions(
      oob:Option[Boolean] = None,
      mTryFraction:Option[Double] =  None,
      bootstrap:Option[Boolean] = None,
      subsample:Option[Double] = None,
      seed:Option[Long] =  None,
      maxDepth:Option[Int] = None,
      minNodeSize:Option[Int] = None, 
      correctImpurity:Option[Boolean] = None,
      airRandomSeed:Option[Long] = None
    ):RandomForestParams = RandomForestParams(oob.getOrElse(true), mTryFraction.getOrElse( Double.NaN), bootstrap.getOrElse(true), 
          subsample.getOrElse(Double.NaN), true, seed.getOrElse(defRng.nextLong), maxDepth.getOrElse(Int.MaxValue), minNodeSize.getOrElse(1),
          correctImpurity.getOrElse(false), airRandomSeed.getOrElse(0L))
}


trait RandomForestCallback {
  def onParamsResolved(actualParams:RandomForestParams) {}
  def onTreeComplete(nTrees:Int, oobError:Double, elapsedTimeMs:Long) {}
}

// TODO (Design): Avoid using type cast change design
trait BatchTreeModel {
  def batchTrain(indexedData: RDD[TreeFeature],  labels: Array[Int], nTryFraction: Double, samples:
    Seq[Sample]): Seq[PredictiveModelWithImportance]
  def batchPredict(indexedData: RDD[TreeFeature], models: Seq[PredictiveModelWithImportance], indexes:Seq[Array[Int]]):
    Seq[Array[Int]]
}

object RandomForest {
  type ModelBuilderFactory = (DecisionTreeParams) => BatchTreeModel

  def wideDecisionTreeBuilder(params:DecisionTreeParams): BatchTreeModel = {
    val decisionTree = new DecisionTree(params)
    new BatchTreeModel() {
      override def batchTrain(indexedData: RDD[TreeFeature], labels: Array[Int], nTryFraction:
        Double, samples:Seq[Sample]) = decisionTree.batchTrainInt(indexedData, labels, nTryFraction, samples)
      override def batchPredict(indexedData: RDD[TreeFeature], models: Seq[PredictiveModelWithImportance], indexes:
        Seq[Array[Int]]) = DecisionTreeModel.batchPredict(indexedData.map(tf => (tf,tf.index)),
              models.asInstanceOf[Seq[DecisionTreeModel]], indexes)
    }
  }

  val defaultBatchSize = 10
}

/** Implements random forest
  * @param params the RF params
  * @param modelBuilderFactory the type of model, i.e. 'wide decision tree builder'
  */
class RandomForest(params:RandomForestParams=RandomForestParams()
      ,modelBuilderFactory:RandomForest.ModelBuilderFactory = RandomForest.wideDecisionTreeBuilder _, trf:TreeRepresentationFactory = DefTreeRepresentationFactory) extends Logging {

  // TODO (Design):make this class keep random state (could be externalised to implicit random)
  implicit lazy val rng = new XorShift1024StarRandomGenerator(params.seed)
  def batchTrain(indexedData: RDD[(Feature, Long)],  labels: Array[Int], nTrees: Int, nBatchSize:Int = RandomForest.defaultBatchSize): RandomForestModel = {
    val treeFeatures:RDD[TreeFeature] = trf.createRepresentation(indexedData)
    batchTrainTyped(treeFeatures, labels, nTrees, nBatchSize)
  }
                    
  // TODO (Design): Make a param rather then an extra method
  // TODO (Func): Add OOB Calculation
  def batchTrainTyped(treeFeatures: RDD[TreeFeature], labels: Array[Int], nTrees: Int, nBatchSize:Int)
                (implicit callback:RandomForestCallback = null): RandomForestModel = {

    require(nBatchSize > 0)
    require(nTrees > 0)   
    val nSamples = labels.length
    val nVariables = treeFeatures.count().toInt
    val nLabels = labels.max + 1

    logDebug(s"Data:  nSamples:${nSamples}, nVariables: ${nVariables}, nLabels:${nLabels}")

    val actualParams = params.resolveDefaults(nSamples, nVariables)

    Option(callback).foreach(_.onParamsResolved(actualParams))
    logDebug(s"Parameters: ${actualParams}")
    logDebug(s"Batch Training: ${nTrees} with batch size: ${nBatchSize}")

    val oobAggregator = if (actualParams.oob) Option(new VotingAggregator(nLabels,nSamples)) else None

    val builder = modelBuilderFactory(actualParams.toDecisionTreeParams(rng.nextLong))
    val allSamples = Stream.fill(nTrees)(Sample.fraction(nSamples, actualParams.subsample, actualParams.bootstrap))

    val (allTrees, errors) = allSamples
      .sliding(nBatchSize, nBatchSize)
      .flatMap { samplesStream =>
        time {

          val samples = samplesStream.toList
          val predictors = builder.batchTrain(treeFeatures, labels, actualParams.nTryFraction, samples)
          val members = if (actualParams.oob) {

            val oobIndexes = samples.map(_.distinctIndexesOut.toArray)
            val oobPredictions = builder.batchPredict(treeFeatures, predictors, oobIndexes)
            predictors.zip(oobIndexes.zip(oobPredictions)).map { case (t, (i,p)) => RandomForestMember(t,i,p)}

          } else predictors.map(RandomForestMember(_))

          val oobError = oobAggregator.map { agg =>
            members.map { m =>
                agg.addVote(m.oobPred, m.oobIndexes)
                Metrics.classificationError(labels, agg.predictions)
            }
          }.getOrElse(List.fill(predictors.size)(Double.NaN))
          members.zip(oobError)
        }.withResultAndTime{ case (treesAndErrors, elapsedTime) =>
          logDebug(s"Trees: ${treesAndErrors.size} >> oobError: ${treesAndErrors.last._2}, time: ${elapsedTime}")
          Option(callback).foreach(_.onTreeComplete(treesAndErrors.size, treesAndErrors.last._2, elapsedTime))
        }.result
     }.toList.unzip

    RandomForestModel(allTrees.toList, nLabels, errors, actualParams)
 }
  
  
}