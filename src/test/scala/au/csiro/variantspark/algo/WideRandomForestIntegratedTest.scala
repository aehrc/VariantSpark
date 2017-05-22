package au.csiro.variantspark.algo

import au.csiro.variantspark.data.BoundedOrdinal
import au.csiro.variantspark.input.generate.{EfectLabelGenerator, OrdinalFeatureGenerator}
import au.csiro.variantspark.test.SparkTest
import org.junit.Assert._
import org.junit.Test


class WideRandomForestIntegratedTest extends SparkTest {

  @Test
  def testFindsImportantVariablesInGeneratedDataset() {
    val seed = 17;
    val fg = OrdinalFeatureGenerator(nLevels = 3, nVariables = 1000, nSamples = 1000, seed = seed)
    val lg = EfectLabelGenerator(fg)(1, Map("v_2" -> 1.0, "v_5" -> 0.75, "v_7" -> 2.0), seed = seed)
    val labels = lg.getLabels(fg.sampleNames)
    val rf = new WideRandomForest(RandomForestParams(seed = seed))
    val data = fg.features().map(_.toVector.values).zipWithIndex.cache
    println(s"Data size: ${data.count}")
    val dataType = BoundedOrdinal(3)
    val rfModel = rf.batchTrain(data, dataType, labels, 200, 50)
    val topThreeVariables = rfModel.variableImportance.toList.sortBy(-_._2).take(3)
    topThreeVariables.foreach(println)
    assertArrayEquals(Array(7L, 2L, 5L), topThreeVariables.unzip._1.toArray)
  }
}