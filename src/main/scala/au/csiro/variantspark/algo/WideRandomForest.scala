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

case class WideRandomForestModel(val trees: List[PredictiveModelWithImportance], val labelCount:Int, oobErrors:List[Double] = List.empty) {
  
  def oobError:Double = oobErrors.last
  
  def printout() {
    trees.zipWithIndex.foreach {
      case (tree, index) =>
        println(s"Tree: ${index}")
        tree.printout()
    }
  }

  def variableImportance: Map[Long, Double] = {   
    // average the importance of each variable over all trees
    // if a variable is not used in a tree it's importance for this tree is assumed to be 0
    trees.map(_.variableImportanceAsFastMap).foldLeft(new Long2DoubleOpenHashMap())(_.addAll(_))
      .asScala.mapValues(_/trees.size)
  }
  
  def predict(data: RDD[Vector]): Array[Int] = predictIndexed(data.zipWithIndex())

  def predictIndexed(indexedData: RDD[(Vector,Long)]): Array[Int] = predictIndexed(indexedData, indexedData.first._1.size)
 
  def predictIndexed(indexedData: RDD[(Vector,Long)], nSamples:Int): Array[Int] = {
     trees.map(_.predictIndexed(indexedData))
       .foldLeft(VotingAggregator(labelCount, nSamples))(_.addVote(_)).predictions
  }
  
}

case class RandomForestParams(
    oob:Boolean = true,
    nTryFraction:Double =  Double.NaN, 
    bootstrap:Boolean = true,
    subsample:Double = Double.NaN
) {
  def resolveDefaults(nSamples:Int, nVariables:Int):RandomForestParams = {
    RandomForestParams(
        oob = oob, 
        nTryFraction = if (!nTryFraction.isNaN) nTryFraction else Math.sqrt(nVariables.toDouble)/nVariables,
        bootstrap = bootstrap,
        subsample = if (!subsample.isNaN) subsample else if (bootstrap) 1.0 else 0.666
    )
  }
}

trait WideRandomForestCallback {
  def onParamsResolved(actualParams:RandomForestParams) {}
  def onTreeComplete(treeIndex:Int, oobError:Double, elapsedTimeMs:Long) {}
}


object WideRandomForest {
  type ModelBuilder = (RDD[(Vector,Long)], Array[Int], Double, Sample) => PredictiveModelWithImportance
  
  def wideDecisionTreeBuilder(indexedData: RDD[(Vector, Long)], labels: Array[Int], nTryFraction: Double, sample:Sample) = new WideDecisionTree().run(indexedData, labels, nTryFraction, sample)
}

class WideRandomForest(params:RandomForestParams=RandomForestParams(),modelBuilder:WideRandomForest.ModelBuilder = WideRandomForest.wideDecisionTreeBuilder) extends Logging {
  def train(indexedData: RDD[(Vector, Long)], labels: Array[Int], nTrees: Int)(implicit callback:WideRandomForestCallback = null): WideRandomForestModel = {
    val nSamples = labels.length
    val nVariables = indexedData.count().toInt
    val nLabels = labels.max + 1  
    logDebug(s"Data:  nSamples:${nSamples}, nVariables: ${nVariables}, nLabels:${nLabels}")
   
    val actualParams = params.resolveDefaults(nSamples, nVariables) 
    Option(callback).foreach(_.onParamsResolved(actualParams))
    logDebug(s"Parameters: ${actualParams}")
   
    val oobAggregator = if (actualParams.oob) Option(new VotingAggregator(nLabels,nSamples)) else None
    
    val (trees, errors) = Range(0, nTrees).map { p =>
      logDebug(s"Building tree: $p")
      time {
        //TODO: Make sure tree accepts sample a indexs not weights !!!
        val sample = Sample.fraction(nSamples, actualParams.subsample, actualParams.bootstrap)
        val tree = modelBuilder(indexedData, labels, actualParams.nTryFraction, sample)
        val oobError = oobAggregator.map { agg =>
          val oobIndexes = sample.indexesOut
          val oobPredictions = tree.predictIndexed(indexedData.project(Projector(oobIndexes.toArray)))
          agg.addVote(oobPredictions, oobIndexes)
          Metrics.classificatoinError(labels, agg.predictions)
        }.getOrElse(Double.NaN)
        (tree, oobError)
      }.withResultAndTime{ case ((tree, error), elapsedTime) =>
        logDebug(s"Tree: ${p} >> oobError: ${error}, time: ${elapsedTime}")
        Option(callback).foreach(_.onTreeComplete(p, error, elapsedTime))
      }.result
    }.unzip
    WideRandomForestModel(trees.toList, nLabels, errors.toList)
  }
}