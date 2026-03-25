package au.csiro.variantspark.algo

import au.csiro.variantspark.data._
import au.csiro.variantspark.test.SparkTest
import au.csiro.variantspark.test.TestFeatureSource
import au.csiro.variantspark.utils.Sample
import org.junit.Test

class PolymorphicDecisionTreeRegressionTest extends SparkTest {

  @Test
  def testTrainPolymorphicRegressionTree() {
    // Smoke test: verify the polymorphic feature pipeline (mixed BoundedOrdinal + Continuous)
    // compiles, runs, and completes when using continuous regression targets.
    val genomicFeatureSource = new TestFeatureSource(Seq(("gen_1", List("0", "0", "1", "2")),
        ("gen_2", List("0", "1", "0", "2"))), BoundedOrdinalVariable(3), ByteArrayDataBuilder)

    val otherFeatureSource =
      new TestFeatureSource(Seq(("cont_1", List("0.2", "0.3", "1.4", "2.5")),
          ("cont_2", List("0.1", "1.3", "0.3", "2.6"))), ContinuousVariable, VectorDataBuilder)

    val allFeatures =
      genomicFeatureSource.features.union(otherFeatureSource.features).zipWithIndex

    val tree = new DecisionTree(DecisionTreeParams(problemType = Regression))
    tree.batchTrain(allFeatures, Array[Double](0.1, 1.3, 0.5, 2.0), 1.0,
      List(Sample.all(allFeatures.first._1.size)))
  }
}
