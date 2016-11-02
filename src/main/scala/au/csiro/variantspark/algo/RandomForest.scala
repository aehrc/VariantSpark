package au.csiro.variantspark.algo

import scala.Range
import scala.collection.JavaConversions.mapAsScalaMap

import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD

import au.csiro.variantspark.metrics.Metrics
import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap
import org.apache.spark.Logging
import au.csiro.variantspark.utils.RDDProjections._
import au.csiro.variantspark.utils.Projector
import au.csiro.pbdava.ssparkle.common.utils.Timed._
import au.csiro.variantspark.utils.Sample
import au.csiro.pbdava.ssparkle.common.utils.FastUtilConversions._
import au.csiro.variantspark.data.VariableType
import it.unimi.dsi.util.XorShift1024StarRandomGenerator
import au.csiro.variantspark.utils.defRng
import org.apache.commons.lang3.builder.ToStringBuilder
import au.csiro.variantspark.utils.IndexedRDDFunction._

import au.csiro.variantspark.algo._
import scala.reflect.ClassTag



trait VarImportanceNormalizer {
  def normalize(varImportance:Map[Long,Double]):Map[Long, Double]
}

case object IdentityVarImportanceNormalizer extends VarImportanceNormalizer {
  override def normalize(varImportance:Map[Long,Double]):Map[Long, Double] = varImportance
}


class StandardImportanceNormalizer(val scale:Double) extends VarImportanceNormalizer {
  override def normalize(varImportance:Map[Long,Double]):Map[Long, Double] = {
    val total = varImportance.values.sum  * scale
    varImportance.mapValues(_/total)
  }
}

case object To100ImportanceNormalizer extends StandardImportanceNormalizer(100.0) 
case object ToOneImportanceNormalizer extends StandardImportanceNormalizer(1.0) 


case class VotingAggregator(val nLabels:Int, val nSamples:Int) {
  lazy val votes = Array.fill(nSamples)(Array.fill(nLabels)(0))
  
  def addVote(predictions:Array[Int], indexes:Iterable[Int]) {
    require(predictions.length <= nSamples, "Valid number of samples")
    predictions.zip(indexes).foreach{ case(v, i) => votes(i)(v) += 1}
  }
  
  def addVote(predictions:Array[Int]):VotingAggregator = {
    require(predictions.length == nSamples, "Full prediction range")
    predictions.zipWithIndex.foreach{ case(v, i) => votes(i)(v) += 1}
    this
  }
  
  def predictions = votes.map(_.zipWithIndex.maxBy(_._1)._2)
}

case class RandomForestModel[V](val trees: List[PredictiveModelWithImportance[V]], val labelCount:Int, oobErrors:List[Double] = List.empty)(implicit canSplit:CanSplit[V]) {
  
  def oobError:Double = oobErrors.last
  
  def printout() {
    trees.zipWithIndex.foreach {
      case (tree, index) =>
        println(s"Tree: ${index}")
        tree.printout()
    }
  }

  def normalizedVariableImportance(norm:VarImportanceNormalizer = To100ImportanceNormalizer): Map[Long, Double] = norm.normalize(variableImportance)
  
  def variableImportance: Map[Long, Double] = {   
    // average the importance of each variable over all trees
    // if a variable is not used in a tree it's importance for this tree is assumed to be 0
    trees.map(_.variableImportanceAsFastMap).foldLeft(new Long2DoubleOpenHashMap())(_.addAll(_))
      .asScala.mapValues(_/trees.size)
  }
  
  def predict(data: RDD[V])(implicit ct:ClassTag[V]): Array[Int] = predictIndexed(data.zipWithIndex())

  def predictIndexed(indexedData: RDD[(V,Long)])(implicit ct:ClassTag[V]): Array[Int] = predictIndexed(indexedData, indexedData.size)
 
  def predictIndexed(indexedData: RDD[(V,Long)], nSamples:Int)(implicit ct:ClassTag[V]): Array[Int] = {
     trees.map(_.predictIndexed(indexedData))
       .foldLeft(VotingAggregator(labelCount, nSamples))(_.addVote(_)).predictions
  }
  
}

case class RandomForestParams(
    oob:Boolean = true,
    nTryFraction:Double =  Double.NaN, 
    bootstrap:Boolean = true,
    subsample:Double = Double.NaN, 
    seed:Long =  defRng.nextLong
) {
  def resolveDefaults(nSamples:Int, nVariables:Int):RandomForestParams = {
    RandomForestParams(
        oob = oob, 
        nTryFraction = if (!nTryFraction.isNaN) nTryFraction else Math.sqrt(nVariables.toDouble)/nVariables,
        bootstrap = bootstrap,
        subsample = if (!subsample.isNaN) subsample else if (bootstrap) 1.0 else 0.666, 
        seed = seed
    )
  }
  override def toString = ToStringBuilder.reflectionToString(this)
}

trait RandomForestCallback {
  def onParamsResolved(actualParams:RandomForestParams) {}
  def onTreeComplete(nTrees:Int, oobError:Double, elapsedTimeMs:Long) {}
}


// TODO: This not nice but at this point in do  not care
// Not nice because I need a type cast
trait BatchTreeModel[V] {
  def batchTrain(indexedData: RDD[(V, Long)], dataType:VariableType, labels: Array[Int], nTryFraction: Double, samples:Seq[Sample]): Seq[PredictiveModelWithImportance[V]]
  def batchPredict(indexedData: RDD[(V, Long)], models: Seq[PredictiveModelWithImportance[V]], indexes:Seq[Array[Int]]): Seq[Array[Int]]  
}

object RandomForest {
  type ModelBuilderFactory[V] = (DecisionTreeParams, CanSplit[V]) => BatchTreeModel[V]
  
  def wideDecisionTreeBuilder[V](params:DecisionTreeParams, canSplit:CanSplit[V]): BatchTreeModel[V] = {
    val decisionTree = new DecisionTree[V](params)(canSplit)
    new BatchTreeModel[V]() {
      override def batchTrain(indexedData: RDD[(V, Long)], dataType:VariableType, labels: Array[Int], nTryFraction: Double, samples:Seq[Sample]) = decisionTree.batchTrain(indexedData, dataType, labels, nTryFraction, samples)
      override def batchPredict(indexedData: RDD[(V, Long)], models: Seq[PredictiveModelWithImportance[V]], indexes:Seq[Array[Int]]) = DecisionTreeModel.batchPredict(indexedData,
              models.asInstanceOf[Seq[DecisionTreeModel[V]]], indexes)(canSplit)
    }
  }
  
  val defaultBatchSize = 10
}

class RandomForest[V](params:RandomForestParams=RandomForestParams()
      ,modelBuilderFactory:RandomForest.ModelBuilderFactory[V] = RandomForest.wideDecisionTreeBuilder[V] _
      )(implicit canSplit:CanSplit[V]) extends Logging {
  
  // TODO (Design): This seems like an easiest solution but it make this class 
  // to keep random state ... perhaps this could be externalised to the implicit random


  
  implicit lazy val rng = new XorShift1024StarRandomGenerator(params.seed)
  
  
  def train(indexedData: RDD[(V, Long)],  dataType: VariableType,  labels: Array[Int], nTrees: Int)(implicit callback:RandomForestCallback = null): RandomForestModel[V] = 
       batchTrain(indexedData, dataType, labels, nTrees, RandomForest.defaultBatchSize)
  
  /**
   * TODO (Nice): Make a parameter rather then an extra method
   * TODO (Func): Add OOB calculation
   */
  def batchTrain(indexedData: RDD[(V, Long)], dataType: VariableType, labels: Array[Int], nTrees: Int, nBatchSize:Int)(implicit callback:RandomForestCallback = null): RandomForestModel[V] = {
    require(nBatchSize > 0)
    require(nTrees > 0)
    val nSamples = labels.length
    val nVariables = indexedData.count().toInt
    val nLabels = labels.max + 1  
    logDebug(s"Data:  nSamples:${nSamples}, nVariables: ${nVariables}, nLabels:${nLabels}")
    val actualParams = params.resolveDefaults(nSamples, nVariables) 
    Option(callback).foreach(_.onParamsResolved(actualParams))
    logDebug(s"Parameters: ${actualParams}")
    logDebug(s"Batch Traning: ${nTrees} with batch size: ${nBatchSize}")
    val oobAggregator = if (actualParams.oob) Option(new VotingAggregator(nLabels,nSamples)) else None   
    
    val builder = modelBuilderFactory(DecisionTreeParams(seed = rng.nextLong), canSplit)    
    val allSamples = Stream.fill(nTrees)(Sample.fraction(nSamples, actualParams.subsample, actualParams.bootstrap))
    val (allTrees, errors) = allSamples
      .sliding(nBatchSize, nBatchSize)
      .flatMap { samplesStream => 
        time {
          val samples = samplesStream.toList
          val predictors = builder.batchTrain(indexedData, dataType, labels, actualParams.nTryFraction, samples)
          val oobError = oobAggregator.map { agg =>
            val oobIndexes = samples.map(_.indexesOut.toArray)
            val oobPredictions = builder.batchPredict(indexedData, predictors, oobIndexes)
            oobPredictions.zip(oobIndexes).map { case(preds, oobIdx) =>
                agg.addVote(preds, oobIdx)
                Metrics.classificatoinError(labels, agg.predictions)
            }
          }.getOrElse(List.fill(predictors.size)(Double.NaN))
          predictors.zip(oobError)
        }.withResultAndTime{ case (treesAndErrors, elapsedTime) =>
          logDebug(s"Trees: ${treesAndErrors.size} >> oobError: ${treesAndErrors.last._2}, time: ${elapsedTime}")
          Option(callback).foreach(_.onTreeComplete(treesAndErrors.size, treesAndErrors.last._2, elapsedTime))
        }.result
     }.toList.unzip
    RandomForestModel(allTrees.toList, nLabels, errors)
 }
  
  
}