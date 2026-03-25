package au.csiro.variantspark.algo

import au.csiro.variantspark.data._
import au.csiro.variantspark.test.SparkTest
import org.apache.spark.mllib.linalg.Vectors
import org.junit.Assert._
import org.junit.Test

class WideRandomForestRegressionTest extends SparkTest {

  val nSamples = 100
  val testData =
    sc.parallelize(List(Vectors.zeros(nSamples))).asFeature(BoundedOrdinalVariable(3))
  val values: Array[Double] = Array.fill(nSamples)(1.0)

  @Test
  def testBuildsCorrectBoostedModelWithoutOob() {
    val nTryFraction = 0.6
    val collector = new TreeDataCollector(
        Stream.continually(TestRegressionPredictorWithImportance(null, null, null)))
    val rf = new RandomForest(RandomForestParams(problemType = Regression, oob = false,
        nTryFraction = nTryFraction, bootstrap = true), modelBuilderFactory = collector.factory)
    val model = rf.batchTrain(testData, values, 10)
    assertEquals("All trees in the model", collector.allTreest, model.trees)

    assertTrue("All trees trained with expected nTryFactor",
      collector.allTryFration.forall(_ == nTryFraction))
    assertTrue("All trees trained same values",
      collector.allResponses.forall {
      case RegressionResponse(v) => v sameElements values
      case _ => false
    })
    assertTrue("All trees trained with requested samples",
      collector.allSamples.forall(s => s.nSize == nSamples && !s.distinctIndexesOut.isEmpty))
  }

  @Test
  def testBuildsCorrectUnBoostedModelWithOob() {
    val nTryFraction = 0.6
    val nTrees = 10
    // Each tree predicts 1.0 for all samples; actual target is also 1.0.
    // OOB RMSE starts at sqrt(0.5) (half the samples unobserved, predicting 0.0 by default)
    // and monotonically decreases towards 0 as more samples acquire OOB coverage.
    val collector = new TreeDataCollector(
        Stream
          .continually(Array.fill(nSamples)(1.0))
          .map(TestRegressionPredictorWithImportance(_, null, null)))
    val rf = new RandomForest(RandomForestParams(problemType = Regression, oob = true,
        nTryFraction = nTryFraction, bootstrap = false, subsample = 0.5),
      modelBuilderFactory = collector.factory)
    val model = rf.batchTrain(testData, values, nTrees)
    assertEquals("All trees in the model", collector.allTreest, model.trees)
    assertTrue("All trees trained with expected nTryFactor",
      collector.allTryFration.forall(_ == nTryFraction))
    assertTrue("All trees trained same values",
      collector.allResponses.forall {
      case RegressionResponse(v) => v sameElements values
      case _ => false
    })
    assertEquals("Oob RMSE errors should always decrease", model.oobErrors.sortBy(-_),
      model.oobErrors)
    assertEquals("The first RMSE should be ~sqrt(0.5)", Math.sqrt(0.5), model.oobErrors.head,
      0.01)
    // With 10 trees at 50% subsampling, ~0.1% of samples may never appear OOB
    // (P(always in-bag) = 0.5^10 ≈ 0.001), giving a residual RMSE of up to sqrt(k/nSamples).
    assertEquals("The last RMSE should be small", 0, model.oobErrors.last, 0.1)
    assertTrue("All trees trained with requested samples",
      collector.allSamples.forall(s => s.length == nSamples / 2 && !s.distinctIndexesOut.isEmpty))
  }
}

