package au.csiro.variantspark.algo

import au.csiro.variantspark.data.BoundedOrdinalVariable
import au.csiro.variantspark.input.generate.{EffectLabelGenerator, OrdinalFeatureGenerator}
import au.csiro.variantspark.test.SparkTest
import org.junit.Assert._
import org.junit.Test
import au.csiro.variantspark.input.CsvLabelSource
import org.apache.hadoop.fs.FileSystem
import au.csiro.variantspark.input.CsvStdFeatureSource
import org.saddle.io.CsvParser
import org.saddle.io.CsvFile

object RandomForestImportanceIntegratedTest {
  val NoTrees:Int = 5000
  val ZQuantiles = Map(1.0 -> 0.6826895, 2.0 ->  0.9544997, 3.0 -> 0.9973002, 4.0 -> 0.9999367)
  
}

class RandomForestImportanceIntegratedTest extends SparkTest {
  import RandomForestImportanceIntegratedTest._
  
  
  implicit val fss = FileSystem.get(sc.hadoopConfiguration)
  implicit val hadoopConf = sc.hadoopConfiguration
  
  def quantile(data:Array[Double])(q:Double):Double = {
    data.filter(_ <= q).size.toDouble/data.size.toDouble
  }
  
  @Test
  def testFindsImportantVariablesInGeneratedDataset() {
    
    val labelSource = new CsvLabelSource("src/test/data/data-labels.csv", "label")
    val featureSource = new CsvStdFeatureSource(sc.textFile("src/test/data/data.csv"))
    val labels = labelSource.getLabels(featureSource.sampleNames)
    val inputData = featureSource.features.zipWithIndex.cache()
    val seed = 13
    val rf = new RandomForest(RandomForestParams(seed = seed))
    val rfModel = rf.batchTrain(inputData,labels, NoTrees, 50)
    println(s"Params: ${rfModel.params}")
    val variableIndex = inputData.map(_.swap).mapValues(_.label).collectAsMap() 
    val variableImporance = rfModel.variableImportance.map(kv => (variableIndex(kv._1), kv._2)).toMap
    println(s"Imp: ${variableImporance}")        
    val importanceStats = CsvParser.parse(CsvFile("src/test/data/data-importance-stats.csv")).withRowIndex(0).withColIndex(0)
    val importanceMeans = importanceStats.firstCol("mean").mapValues(CsvParser.parseDouble).toSeq.toMap
    val importanceSds = importanceStats.firstCol("sd").mapValues(CsvParser.parseDouble).toSeq.toMap
    println(s"Means: ${importanceMeans}")
    println(s"Sds: ${importanceSds}")
    
    val residualsAbs =  variableImporance.map({ case (k, v) => (k, Math.abs(v - importanceMeans(k))/importanceSds(k))})
    val importanceQuantile = quantile(residualsAbs.values.toArray)(_)
    ZQuantiles.foreach{ case (q, p) =>
      println(importanceQuantile(q), p)
    }
  }
}