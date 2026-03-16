package au.csiro.variantspark.algo

/** Base trait for aggregating predictions from multiple trees in a random forest.
  */
trait PredictionAggregator {
  def addPredictions(predictions: Array[Any]): PredictionAggregator
  def addPredictions(predictions: Array[Any], indexes: Iterable[Int]): Unit
  def predictions: Array[Any]
}

/** Implements voting aggregator for classification.
  *
  * @param nLabels the number of labels
  * @param nSamples the number of samples
  */
case class VotingAggregator(nLabels: Int, nSamples: Int) extends PredictionAggregator {
  lazy val votes: Array[Array[Int]] = Array.fill(nSamples)(Array.fill(nLabels)(0))

  /** Adds a vote with predictions and indexes
    * @param predictions the number of predictions
    * @param indexes the number of indexes
    */
  def addPredictions(predictions: Array[Any], indexes: Iterable[Int]): Unit = {
    require(predictions.length <= nSamples, "Valid number of samples")
    predictions.zip(indexes).foreach { case (v, i) => votes(i)(v.asInstanceOf[Int]) += 1 }
  }

  /** Adds a vote with predictions
    * @param predictions the number of predictions
    */
  def addPredictions(predictions: Array[Any]): VotingAggregator = {
    require(predictions.length == nSamples, "Full prediction range")
    predictions.zipWithIndex.foreach { case (v, i) => votes(i)(v.asInstanceOf[Int]) += 1 }
    this
  }

  /** Maps votes to majority-class predictions */
  def predictions: Array[Any] = votes.map(v => (v.indices.maxBy(v): Any))

  /**
    * Computes class probabilities.
    * The result is an array with one item per sample, where
    * each item is a vector with class probabilities for this sample.
    * @return predicted class probabilities for each sample.
    */
  def classProbabilities: Array[Array[Double]] = {
    votes.map { row =>
      val sampleTotal = row.sum.toDouble
      row.map(classCount => classCount / sampleTotal)
    }
  }
}

/** Implements averaging aggregator for regression.
  *
  * @param nSamples the number of samples
  */
case class AveragingAggregator(nSamples: Int) extends PredictionAggregator {
  lazy val values: Array[Double] = Array.fill(nSamples)(0.0)
  lazy val counts: Array[Int] = Array.fill(nSamples)(0)

  /** Adds predictions for out-of-bag indexed samples
    * @param predictions an array of continuous predictions
    * @param indexes the sample indexes to update
    */
  def addPredictions(predictions: Array[Any], indexes: Iterable[Int]): Unit = {
    require(predictions.length <= nSamples, "Valid number of samples")
    predictions.zip(indexes).foreach {
      case (v, i) =>
        values(i) += v.asInstanceOf[Double]
        counts(i) += 1
    }
  }

  /** Adds a full-range prediction pass
    * @param predictions an array of continuous predictions
    */
  def addPredictions(predictions: Array[Any]): AveragingAggregator = {
    require(predictions.length == nSamples, "Full prediction range")
    var i = 0
    while (i < nSamples) {
      values(i) += predictions(i).asInstanceOf[Double]
      counts(i) += 1
      i += 1
    }
    this
  }

  /** Maps sums and counts to mean predictions */
  def predictions: Array[Any] = {
    val result = new Array[Any](nSamples)
    var i = 0
    while (i < nSamples) {
      result(i) = if (counts(i) > 0) values(i) / counts(i) else 0.0
      i += 1
    }
    result
  }
}

/**
  * Creates [[PredictionAggregator]] instances for a given sample count.
  * Serializable so it can be broadcast to Spark workers.
  */
trait PredictionAggregatorFactory extends Serializable {
  def create(nSamples: Int): PredictionAggregator
}

case class VotingAggregatorFactory(nCategories: Int) extends PredictionAggregatorFactory {
  def create(nSamples: Int): PredictionAggregator = VotingAggregator(nCategories, nSamples)
}

case object AveragingAggregatorFactory extends PredictionAggregatorFactory {
  def create(nSamples: Int): PredictionAggregator = AveragingAggregator(nSamples)
}
