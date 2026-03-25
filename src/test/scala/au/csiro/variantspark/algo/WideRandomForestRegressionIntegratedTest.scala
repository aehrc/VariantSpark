package au.csiro.variantspark.algo

import au.csiro.variantspark.data.BoundedOrdinalVariable
import au.csiro.variantspark.input.generate.{EffectLabelGenerator, OrdinalFeatureGenerator}
import au.csiro.variantspark.test.SparkTest
import org.junit.Assert._
import org.junit.Test

object WideRandomForestRegressionIntegratedTest {
  def NoTrees: Int = 200
  def NoSamples: Int = 1000
}

class WideRandomForestRegressionIntegratedTest extends SparkTest {
  import WideRandomForestRegressionIntegratedTest._

  @Test
  def testFindsImportantVariablesInGeneratedDataset() {
    val seed = 17
    val fg =
      OrdinalFeatureGenerator(nLevels = 3, nVariables = 1000, nSamples = NoSamples, seed = seed)
    val lg =
      EffectLabelGenerator(fg)(1, Map("v_2" -> 1.0, "v_5" -> 0.75, "v_7" -> 2.0), seed = seed)

    // Trigger computation of the continuous linear combination (pre-logistic).
    // We use this as the regression target directly, so the effect sizes are exactly
    // reflected in the response variance and the importance ranking is [7, 2, 5].
    lg.getResponses(fg.sampleNames)
    val values: Array[Double] = lg.continouusResponse.toArray

    val rf = new RandomForest(RandomForestParams(problemType = Regression, seed = seed))
    val data = fg.features.zipWithIndex.cache
    val rfModel = rf.batchTrain(data, values, NoTrees, 50)
    val topThreeVariables = rfModel.variableImportance.toList.sortBy(-_._2).take(3)
    topThreeVariables.foreach(println)
    assertArrayEquals(Array(7L, 2L, 5L), topThreeVariables.unzip._1.toArray)
    // additional check on the model itself
    assertEquals(NoTrees, rfModel.members.size)
    rfModel.members.foreach { p =>
      assertEquals("All trees are built on a bootstrapped sample of original size", NoSamples,
        p.predictor.asInstanceOf[DecisionTreeModel].rootNode.size)
    }
  }
}
