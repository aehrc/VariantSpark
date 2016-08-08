package au.csiro.variantspark.algo

import scala.Range
import scala.collection.JavaConversions.mapAsScalaMap

import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD

import au.csiro.variantspark.metrics.Metrics
import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap
import org.apache.spark.Logging

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
  
  def predict(data: RDD[Vector]): Array[Int] = {
    val sampleCount = data.first.size
    // for classification we just do majority vote
    val votes = Array.fill(sampleCount)(Array.fill(labelCount)(0))
    trees.map(_.predict(data)).foreach { x => x.zipWithIndex.foreach{ case (v, i) => votes(i)(v)+=1}} // this is each tree vote for eeach sample
    // now for each sample find the label with the highest count
    votes.map(_.zipWithIndex.max._2)
  }

  def predictIndexed(data: RDD[(Vector,Long)]): Array[Int] = {
    val sampleCount = data.first._1.size
    // for classification we just do majority vote
    val votes = Array.fill(sampleCount)(Array.fill(labelCount)(0))
    trees.map(_.predictIndexed(data)).foreach { x => x.zipWithIndex.foreach{ case (v, i) => votes(i)(v)+=1}} // this is each tree vote for eeach sample
    // now for each sample find the label with the highest count
    votes.map(_.zipWithIndex.max._2)
  }
  
}

case class RandomForestParams(
    oob:Boolean = true,
    nTryFraction:Double =  Double.NaN
)

class WideRandomForest extends Logging {
  def run(data: RDD[(Vector, Long)], labels: Array[Int], ntrees: Int, params:RandomForestParams = RandomForestParams()): WideRandomForestModel = {
    // subsample
    //dims seems to be the number of samples, not number of dimensions?
    val dims = labels.length
    val features = data.count().toInt
    val labelCount = labels.max + 1
    
    
    val oobVotes = Array.fill(dims)(Array.fill(labelCount)(0))
    logDebug("Features: " + features.toDouble)
    val ntryFraction = if (params.nTryFraction.isNaN ) Math.sqrt(features.toDouble)/features.toDouble else params.nTryFraction
    logDebug(s"RF: Using ntryfraction: $ntryFraction")
    logDebug(s"RF: i.e. trying ${(features * ntryFraction).toInt} features per split")

    val trees = Range(0, ntrees).map { p =>
      logDebug(s"Building tree: $p")
      //val currentSample = for (i <- 0 until dims) yield (Math.random * dims).toInt // sample with replacement
      val currentSample = IndexedSeq.fill(dims)((Math.random * dims).toInt) // sample with replacement
      val tree = new WideDecisionTree().run(data, labels, currentSample.toArray, ntryFraction)
      val error = if (params.oob) {
        // check which indexes are out of bag
        val inBag = currentSample.distinct.toSet // 
        logDebug(s"oob size: ${dims-inBag.size}")
        // predict on projected data
        // tree.predict() on projected data
        // looks like I actually need to get the reversed set anyway
        val oobIndexes = Range(0,dims).toSet.diff(inBag)
        val predictions = tree.predictIndexed(data.map( t => (WideDecisionTree.projectVector(oobIndexes, invert = false)(t._1), t._2)))
        val indexes = oobIndexes.toSeq.sorted
        predictions.zip(indexes).foreach{ case(v, i) => oobVotes(i)(v) += 1}
        Metrics.classificatoinError(labels, oobVotes.map(_.zipWithIndex.max._2))
      } else {
        Double.NaN
      }
      logDebug(s"Tree error: $error")
      (tree, error)
    }
    val oobError = trees.map(_._2).sum.toDouble / ntrees
    logDebug(s"Error: oobError")
    WideRandomForestModel(trees.map(_._1).toList, labelCount, oobError)
  }
}