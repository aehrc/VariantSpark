package au.csiro.variantspark.algo

import au.csiro.variantspark.data.Feature
import au.csiro.variantspark.utils.Sample
import org.apache.spark.rdd.RDD

import scala.collection.mutable.MutableList

class TreeDataCollector(
    treeStream: Stream[PredictiveModelWithImportance] = Stream.continually(
        TestPredictorWithImportance(null, null, null)))
    extends BatchTreeModel {
  val allTypedData = MutableList[RDD[TreeFeature]]()
  val allLabels = MutableList[ResponseVariable]()
  val allTryFration = MutableList[Double]()
  val allSamples = MutableList[Sample]()
  val allTreest = MutableList[PredictiveModelWithImportance]()
  val treeIter = treeStream.toIterator

  override def batchTrain(indexedData: RDD[TreeFeature], response: ResponseVariable,
      nTryFraction: Double, samples: Seq[Sample]): Seq[PredictiveModelWithImportance] = {
    allTypedData += indexedData
    allLabels += response
    allTryFration += nTryFraction
    allSamples ++= samples
    val newTrees = treeIter.take(samples.size).toSeq
    allTreest ++= newTrees
    newTrees
  }

  override def batchPredict(indexedTypedData: RDD[TreeFeature],
      models: Seq[PredictiveModelWithImportance], indexes: Seq[Array[Int]]): Seq[Array[Any]] = {
    // TODO I should be projecting with indexes here
    // but it does not matter in this case
    models.zip(indexes).map {
      case (model, indexes) =>
        model.predict(indexedTypedData.map(tf => (tf.asInstanceOf[Feature], tf.index)))
    }
  }

  def factory(params: DecisionTreeParams) = this
}
