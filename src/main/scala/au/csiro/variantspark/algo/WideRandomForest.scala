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

class VotingAggregator(val nLabels:Int, val nSamples:Int) {
  lazy val votes = Array.fill(nSamples)(Array.fill(nLabels)(0))
  
  def addVote(predictions:Array[Int], indexes:Iterable[Int]) {
    require(predictions.length <= nSamples, "Valid number of samples")
    predictions.zip(indexes).foreach{ case(v, i) => votes(i)(v) += 1}
  }
  
  def addVote(predictions:Array[Int]) {
    require(predictions.length == nSamples, "Full prediction range")
    predictions.zipWithIndex.foreach{ case(v, i) => votes(i)(v) += 1}
  }
  
  def predictions = votes.map(_.zipWithIndex.max._2)
}

case class WideRandomForestModel(trees: List[WideDecisionTreeModel], val labelCount:Int, oobError:Double) {
  def printout() {
    trees.zipWithIndex.foreach {
      case (tree, index) =>
        println(s"Tree: ${index}")
        tree.printout()
    }
  }

  def variableImportance: Map[Long, Double] = {
    // average by all trees
    val accumulations = new Long2DoubleOpenHashMap()
    val counts = new Long2IntOpenHashMap()
    trees.foreach { t =>
      val treeImportnace = t.variableImportanceAsFastMap
      treeImportnace.foreach {
        case (index, imp) =>
          accumulations.addTo(index, imp)
          counts.addTo(index, 1)
      }
    }
    accumulations.map { case (index, value) => (index.toLong, value.toDouble/trees.size) }.toMap
  }
  
  def predict(data: RDD[Vector]): Array[Int] = predictIndexed(data.zipWithIndex())

  def predictIndexed(indexedData: RDD[(Vector,Long)]): Array[Int] = {
    val agg = new VotingAggregator(labelCount, indexedData.first._1.size)
    trees.map(_.predictIndexed(indexedData)).foreach(agg.addVote)
    agg.predictions
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
  def onTreeComplete(treeIndex:Int, oobError:Double, elapsedTimeMs:Long)
}

class WideRandomForest(params:RandomForestParams = RandomForestParams()) extends Logging {
  def train(indexedData: RDD[(Vector, Long)], labels: Array[Int], nTrees: Int)(implicit callback:WideRandomForestCallback = null): WideRandomForestModel = {
    val nSamples = labels.length
    val nVariables = indexedData.count().toInt
    val nLabels = labels.max + 1  
    logDebug(s"Data:  nSamples:${nSamples}, nVariables: ${nVariables}, nLabels:${nLabels}")
   
    val actualParams = params.resolveDefaults(nSamples, nVariables)  
    logDebug(s"Parameters: ${actualParams}")
   
    val oobAggregator = if (actualParams.oob) Option(new VotingAggregator(nLabels,nSamples)) else None
    
    val (trees, errors) = Range(0, nTrees).map { p =>
      logDebug(s"Building tree: $p")
      time {
        //TODO: Make sure tree accepts sample a indexs not weights !!!
        val sample = Sample.fraction(nSamples, actualParams.subsample, actualParams.bootstrap)
        val tree = new WideDecisionTree().run(indexedData, labels, sample.indexes, actualParams.nTryFraction)
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
    WideRandomForestModel(trees.toList, nLabels, errors.last)
  }
}