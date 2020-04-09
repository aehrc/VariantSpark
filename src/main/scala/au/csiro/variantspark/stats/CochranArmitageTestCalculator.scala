package au.csiro.variantspark.stats

import org.apache.spark.rdd.RDD
import au.csiro.pbdava.ssparkle.spark.SparkUtils.withBroadcast

class CochranArmitageTestCalculator(val labels: Array[Int], val weights: Array[Double]) {
  val tester = CochranArmitageTest.get()

  def pScore(values: Array[Byte]): Double = {

    require(values.length == labels.length)
    require(labels.max < 2)
    val inputClassesNo = values.max + 1
    require(inputClassesNo <= weights.length)

    // only works for two classes, so labels need to be 0 and 1
    val confusionMatrix = Array.fill(2)(Array.fill(weights.length)(0))
    labels.zip(values).foreach { case (li, vi) => confusionMatrix(li)(vi) += 1 }
    tester.p(confusionMatrix(0), confusionMatrix(1), weights)
  }
}

object CochranArmitageTestCalculator {
  val WEIGHT_TREND = CochranArmitageTest.WEIGHT_TREND
}

class CochranArmitageTestScorer(val labels: Array[Int], val weights: Array[Double],
    val top: Int = 20) {

  /**
    * This implements a CochranArmitageTest for threads
    * see: https://en.wikipedia.org/wiki/Cochran%E2%80%93Armitage_test_for_trend
    */
  def topN(indexedData: RDD[(Array[Byte], Long)]): Array[(Long, Double)] = {
    withBroadcast(indexedData.context)(labels) { br_labels =>
      withBroadcast(indexedData.context)(weights) { br_weights =>
        indexedData
          .mapPartitions({ it =>
            val test = new CochranArmitageTestCalculator(br_labels.value, br_weights.value)
            it.map { case (values, i) => (i, test.pScore(values)) }
          })
          .sortBy(_._2, ascending = true)
          .take(top)
      }
    }
  }
}
