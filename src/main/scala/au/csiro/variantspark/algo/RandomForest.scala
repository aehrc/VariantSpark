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

/** Allows for normalization(scaling)of the input map values
  */
trait VarImportanceNormalizer {
  def normalize(varImportance:Map[Long,Double]):Map[Long, Double]
}

/** Defines normalization variable conditionally
  */
case object IdentityVarImportanceNormalizer extends VarImportanceNormalizer {
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

  /** Implements adding a vote
    * @param predictions the number of predictions
    * @param indexes the number of indexes
    */
  def addVote(predictions:Array[Int], indexes:Iterable[Int]) {
    require(predictions.length <= nSamples, "Valid number of samples")
    predictions.zip(indexes).foreach{ case(v, i) => votes(i)(v) += 1}
  }

  /** Implements adding a vote
    * @param predictions the number of predictions
    */
  def addVote(predictions:Array[Int]):VotingAggregator = {
    require(predictions.length == nSamples, "Full prediction range")
    predictions.zipWithIndex.foreach{ case(v, i) => votes(i)(v) += 1}
    this
  }
  
  def predictions = votes.map(_.zipWithIndex.maxBy(_._1)._2)
}

/** Implements random forest members conditionally
  * @param predictor the predictor model
  * @param oobIndexes an array of out-of-bag index values
  */
@SerialVersionUID(2l)
case class RandomForestMember[V](val predictor:PredictiveModelWithImportance[V],
                                 val oobIndexes:Array[Int] = null, val oobPred:Array[Int] = null) {
}

/** Implements random forest models conditionally
  * @param members the RF members
  * @param labelCount the label count
  * @param oobErrors the out-of-bag erros
  */
@SerialVersionUID(2l)
case class RandomForestModel[V](val members: List[RandomForestMember[V]], val labelCount:Int, val oobErrors:
  List[Double] = List.empty)(implicit canSplit:CanSplit[V]) {

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

  def variableImportance: Map[Long, Double] = {

    // average the importance of each variable over all trees
    // if a variable is not used in a tree it's importance for this tree is assumed to be 0

    trees.map(_.variableImportanceAsFastMap).foldLeft(new Long2DoubleOpenHashMap())(_.addAll(_))
      .asScala.mapValues(_/size)
  }

  def predict(data: RDD[V])(implicit ct:ClassTag[V]): Array[Int] = predictIndexed(data.zipWithIndex())

  def predictIndexed(indexedData: RDD[(V,Long)])(implicit ct:ClassTag[V]):
    Array[Int] = predictIndexed(indexedData, indexedData.size)

  def predictIndexed(indexedData: RDD[(V,Long)], nSamples:Int)(implicit ct:ClassTag[V]): Array[Int] = {
     trees.map(_.predictIndexed(indexedData))
       .foldLeft(VotingAggregator(labelCount, nSamples))(_.addVote(_)).predictions
  }

}

/** Implements random forest params conditionally
  * @param oob the out-of-bag value
  * @param nTryFraction the n-try fraction value
  * @param bootstrap the bootstrap value
  * @param subsample the subsample value
  * @param seed the seed value
  */
case class RandomForestParams(
    oob:Boolean = true,
    nTryFraction:Double =  Double.NaN,
    bootstrap:Boolean = true,
    subsample:Double = Double.NaN,
    randomizeEquality:Boolean = false,
    seed:Long =  defRng.nextLong
) {
  def resolveDefaults(nSamples:Int, nVariables:Int):RandomForestParams = {
    RandomForestParams(
        oob = oob,
        nTryFraction = if (!nTryFraction.isNaN) nTryFraction else Math.sqrt(nVariables.toDouble)/nVariables,
        bootstrap = bootstrap,
        subsample = if (!subsample.isNaN) subsample else if (bootstrap) 1.0 else 0.666,
        randomizeEquality  = randomizeEquality,
        seed = seed
    )
  }
  override def toString = ToStringBuilder.reflectionToString(this)
}

trait RandomForestCallback {
  def onParamsResolved(actualParams:RandomForestParams) {}
  def onTreeComplete(nTrees:Int, oobError:Double, elapsedTimeMs:Long) {}
}

// TODO (Design): Avoid using type cast change design
trait BatchTreeModel[V] {
  def batchTrain(indexedData: RDD[(V, Long)], dataType:VariableType, labels: Array[Int], nTryFraction: Double, samples:
    Seq[Sample]): Seq[PredictiveModelWithImportance[V]]
  def batchPredict(indexedData: RDD[(V, Long)], models: Seq[PredictiveModelWithImportance[V]], indexes:Seq[Array[Int]]):
    Seq[Array[Int]]
}

object RandomForest {
  type ModelBuilderFactory[V] = (DecisionTreeParams, CanSplit[V]) => BatchTreeModel[V]

  def wideDecisionTreeBuilder[V](params:DecisionTreeParams, canSplit:CanSplit[V]): BatchTreeModel[V] = {
    val decisionTree = new DecisionTree[V](params)(canSplit)
    new BatchTreeModel[V]() {
      override def batchTrain(indexedData: RDD[(V, Long)], dataType:VariableType, labels: Array[Int], nTryFraction:
        Double, samples:Seq[Sample]) = decisionTree.batchTrain(indexedData, dataType, labels, nTryFraction, samples)
      override def batchPredict(indexedData: RDD[(V, Long)], models: Seq[PredictiveModelWithImportance[V]], indexes:
        Seq[Array[Int]]) = DecisionTreeModel.batchPredict(indexedData,
              models.asInstanceOf[Seq[DecisionTreeModel[V]]], indexes)(canSplit)
    }
  }

  val defaultBatchSize = 10
}

/** Implements random forest
  * @param params the RF params
  * @param modelBuilderFactory the type of model, i.e. 'wide decision tree builder'
  */
class RandomForest[V](params:RandomForestParams=RandomForestParams()
      ,modelBuilderFactory:RandomForest.ModelBuilderFactory[V] = RandomForest.wideDecisionTreeBuilder[V] _
      )(implicit canSplit:CanSplit[V]) extends Logging {

  // TODO (Design):easiest solution -> makes this class keep random state, but could be externalised to implicit random
  implicit lazy val rng = new XorShift1024StarRandomGenerator(params.seed)

  def train(indexedData: RDD[(V, Long)],  dataType: VariableType,  labels: Array[Int], nTrees: Int)
           (implicit callback:RandomForestCallback = null): RandomForestModel[V] =
              batchTrain(indexedData, dataType, labels, nTrees, RandomForest.defaultBatchSize)


  // TODO (Nice to do): Make a param rather then an extra method, (Func) Add OOB Calculation
  def batchTrain(indexedData: RDD[(V, Long)], dataType: VariableType, labels: Array[Int], nTrees: Int, nBatchSize:Int)
                (implicit callback:RandomForestCallback = null): RandomForestModel[V] = {

    require(nBatchSize > 0)
    require(nTrees > 0)

    val nSamples = labels.length
    val nVariables = indexedData.count().toInt
    val nLabels = labels.max + 1

    logDebug(s"Data:  nSamples:${nSamples}, nVariables: ${nVariables}, nLabels:${nLabels}")

    val actualParams = params.resolveDefaults(nSamples, nVariables)

    Option(callback).foreach(_.onParamsResolved(actualParams))
    logDebug(s"Parameters: ${actualParams}")
    logDebug(s"Batch Training: ${nTrees} with batch size: ${nBatchSize}")

    val oobAggregator = if (actualParams.oob) Option(new VotingAggregator(nLabels,nSamples)) else None

    val builder = modelBuilderFactory(DecisionTreeParams(seed = rng.nextLong, randomizeEquality = actualParams
      .randomizeEquality), canSplit)
    val allSamples = Stream.fill(nTrees)(Sample.fraction(nSamples, actualParams.subsample, actualParams.bootstrap))

    val (allTrees, errors) = allSamples
      .sliding(nBatchSize, nBatchSize)
      .flatMap { samplesStream =>
        time {

          val samples = samplesStream.toList
          val predictors = builder.batchTrain(indexedData, dataType, labels, actualParams.nTryFraction, samples)
          val members = if (actualParams.oob) {

            val oobIndexes = samples.map(_.indexesOut.toArray)
            val oobPredictions = builder.batchPredict(indexedData, predictors, oobIndexes)
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

    RandomForestModel(allTrees.toList, nLabels, errors)
 }
  
  
}