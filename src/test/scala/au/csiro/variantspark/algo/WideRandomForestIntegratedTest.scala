package au.csiro.variantspark.algo

import org.junit.Assert._
import org.junit.Test
import au.csiro.variantspark.test.SparkTest
import au.csiro.pbdava.ssparkle.common.utils.FastUtilConversions._
import au.csiro.variantspark.utils.VectorRDDFunction._
import org.saddle.io._
import org.saddle._
import org.saddle.io.CsvImplicits._
import au.csiro.variantspark.data.BoundedOrdinal
import au.csiro.variantspark.input.generate.OrdinalFeatureSource
import au.csiro.variantspark.input.generate.EfectLabelGenerator
import it.unimi.dsi.util.XorShift1024StarRandomGenerator


class WideRandomForestIntegratedTest extends SparkTest {
  
  @Test
  def testFindsImportantVariablesInGeneratedDataset() {
    val seed = 17;
    val fg = OrdinalFeatureSource(nLevels = 3, nVariables = 1000, nSamples = 1000, seed = seed)
    val lg = EfectLabelGenerator(1, 0, Map(2L->1.0, 5L->0.75, 7L->2.0), fg, seed = seed)
    val labels = lg.getLabels(fg.sampleNames)
    val rf = new WideRandomForest()
    val data = fg.features().map(_.toVector.values).zipWithIndex.cache
    println(s"Data size: ${data.count}")   
    val dataType = BoundedOrdinal(3)
    val rfModel = rf.batchTrain(data, dataType, labels, 200, 50)
    val topThreeVariables = rfModel.variableImportance.toList.sortBy(-_._2).take(3)
    topThreeVariables.foreach(println)
    assertArrayEquals(Array(7L, 2L, 5L), topThreeVariables.unzip._1.toArray)
  }
}