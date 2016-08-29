package au.csiro.variantspark.input.generate

import org.junit.Assert._
import org.junit.Test;
import au.csiro.variantspark.test.SparkTest
import au.csiro.variantspark.algo.WideRandomForest
import au.csiro.pbdava.ssparkle.common.utils.FastUtilConversions._
import au.csiro.variantspark.utils.VectorRDDFunction._
import au.csiro.variantspark.algo.WideDecisionTree
import au.csiro.variantspark.utils.Sample
import au.csiro.variantspark.output.CSVFeatureSink
import org.saddle.io._
import org.saddle._
import org.saddle.io.CsvImplicits._


class OrdinalFeatureSourceTest extends SparkTest {
  @Test
  def testGeneratesNiceData() {
    val fg = OrdinalFeatureSource(nLevels = 3, nVariables = 100, nSamples = 1000)
    val lg = EfectLabelGenerator(0, Map(2L->5.0), fg)
    val labels = lg.getLabels(fg.sampleNames)
    val rf = new WideRandomForest()
    val data = fg.features().map(_.toVector.values).zipWithIndex.cache
    val data2 = data.collectAtIndexes(Set(2L))    
    //CSVFeatureSink("tmp/gendata.csv").save(fg)
    val df = Frame("label" -> Vec(labels))
    //df.writeCsvFile("tmp/labels.csv", withColIx = true, withRowIx = false)
    val rfModel = rf.train(data, labels, 100)
    rfModel.variableImportance.toList.sortBy(-_._2).take(20).foreach(println)
    
    //val dt = new WideDecisionTree()
    //val m = dt.run(data, labels, 1.0, Sample.all(1000))
    //m.variableImportance.toList.sortBy(-_._2).take(20).foreach(println)

  }
}