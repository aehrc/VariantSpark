package au.csiro.variantspark.algo

import au.csiro.variantspark.data.VariableType
import au.csiro.variantspark.utils.Sample
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD

import scala.collection.mutable.MutableList
import au.csiro.variantspark.data.Feature

class TreeDataCollector(
    treeStream: Stream[PredictiveModelWithImportance] =
      Stream.continually(TestPredictorWithImportance(null, null)))
    extends BatchTreeModel {
  val allTypedData = MutableList[RDD[TreeFeature]]()
  val allLabels = MutableList[Array[Int]]()
  val allTryFration = MutableList[Double]()
  val allSamples = MutableList[Sample]()
  val allTreest = MutableList[PredictiveModelWithImportance]()
  val treeIter = treeStream.toIterator

  override def batchTrain(
      indexedData: RDD[TreeFeature],
      labels: Array[Int],
      nTryFraction: Double,
      samples: Seq[Sample]): Seq[PredictiveModelWithImportance] = {
    allTypedData += indexedData
    allLabels += labels
    allTryFration += nTryFraction
    allSamples ++= samples
    val newTrees = treeIter.take(samples.size).toSeq
    allTreest ++= newTrees
    newTrees
  }

  override def batchPredict(
      indexedTypedData: RDD[TreeFeature],
      models: Seq[PredictiveModelWithImportance],
      indexes: Seq[Array[Int]]): Seq[Array[Int]] = {
    //TODO I should be projecting with indexes here
    //but it does not matter in this case
    models.zip(indexes).map {
      case (model, indexes) =>
        model.predict(indexedTypedData.map(tf => (tf.asInstanceOf[Feature], tf.index)))
    }
  }

  def factory(params: DecisionTreeParams) = this
}
