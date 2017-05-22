package au.csiro.variantspark.algo

import au.csiro.variantspark.data.VariableType
import au.csiro.variantspark.utils.Sample
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD

import scala.collection.mutable.MutableList

class TreeDataCollector(treeStream: Stream[PredictiveModelWithImportance[Vector]] = Stream.continually(TestPredictorWithImportance(null, null))) extends BatchTreeModel[Vector] {
  val allData = MutableList[RDD[(Vector, Long)]]()
  val allLabels = MutableList[Array[Int]]()
  val allnTryFration = MutableList[Double]()
  val allSamples = MutableList[Sample]()
  val allTreest = MutableList[PredictiveModelWithImportance[Vector]]()
  val treeIter = treeStream.toIterator

  override def batchTrain(indexedData: RDD[(Vector, Long)], dataType: VariableType, labels: Array[Int], nTryFraction: Double, samples: Seq[Sample]): Seq[PredictiveModelWithImportance[Vector]] = {
    allData += indexedData
    allLabels += labels
    allnTryFration += nTryFraction
    allSamples ++= samples
    val newTrees = treeIter.take(samples.size).toSeq
    allTreest ++= newTrees
    newTrees
  }

  override def batchPredict(indexedData: RDD[(Vector, Long)], models: Seq[PredictiveModelWithImportance[Vector]], indexes: Seq[Array[Int]]): Seq[Array[Int]] = {
    //TODO I should be projecting with indexes here
    //but it does not matter in this case
    models.zip(indexes).map { case (model, indexes) => model.predictIndexed(indexedData) }
  }

  def factory(params: DecisionTreeParams, canSplit: CanSplit[Vector]) = this
}
