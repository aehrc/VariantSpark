package au.csiro.variantspark.test.stats

import au.csiro.variantspark.test.AbstractCmdLineTest
import org.junit.Test
import org.junit.Assert._
import java.io.File
import org.saddle.io.CsvParser
import org.saddle.io.CsvFile
import au.csiro.pbdava.ssparkle.common.utils.CSVUtils
import java.io.FileOutputStream
import au.csiro.variantspark.test.TestData

class ImportanceStatsTest extends AbstractCmdLineTest with TestData {
  
  import ImportanceStatsTest._
  
  def subdir = "stats"

  def quantile(data:Array[Double])(q:Double):Double = {
    data.filter(_ <= q).size.toDouble/data.size.toDouble
  }
  
  @Test
  def testStats() {
    val labelsFile = data("stats_100_1000_cont_0.0-labels_null.csv")
    val dataFile = data("stats_100_1000_cont_0.0-wide.csv")
    val outputFile = actual("stats_100_1000_cont_0.0-importance.csv")
    val nTrees = 2000
    
    runVariantSpark(s"""importance -if ${dataFile} -ff ${labelsFile} -fc cat2 -it csv -v -rn ${nTrees} -rbs 250 -sr 13 -on 0 -ovn raw -sp 4 -of ${outputFile}""")   
    
    val importanceStats = CsvParser.parse(CsvFile("src/test/data/stats/stats_100_1000_cont_0.0-stats.csv")).withRowIndex(0).withColIndex(0)
    val importanceMeans = importanceStats.firstCol("mean").mapValues(CsvParser.parseDouble).toSeq.toMap
    val importanceSds = importanceStats.firstCol("sd").mapValues(CsvParser.parseDouble).toSeq.toMap
    
    val actualImportances = CsvParser.parse(CsvFile(outputFile)).withRowIndex(0).withColIndex(0)
    val variableImportance = actualImportances.firstCol("importance").mapValues(CsvParser.parseDouble).toSeq.toMap

    println(s"Means: ${importanceMeans}")
    println(s"Sds: ${importanceSds}")
    println(s"Importances: ${variableImportance}")
    val residuals =  variableImportance.map({ case (k, v) => (k, (v - importanceMeans(k))/importanceSds(k))})
    val residualsAbs = residuals.mapValues(Math.abs)   
    
    CSVUtils.withStream(new FileOutputStream(actual("stats_100_1000_cont_0.0-residual.csv"))) { writer =>
      val header = List("variable","residual")
      writer.writeRow(header)
      writer.writeAll(residuals.toList.sortBy(_._1).map(_.productIterator.toSeq).toSeq)
    }   
    
    val importanceQuantile = quantile(residualsAbs.values.toArray)(_)

    // Check that estimated quantiles are not much smaller than the normal ones.
    ZQuantiles.foreach{ case (q, p) =>
      assertTrue(importanceQuantile(q) > p - 0.01)
    }
    
    // TODO: Add check on correlation (shoiuld be less than 0.2)
    // Use: org.apache.commons.math3.stat.correlation.PearsonsCorrelation
  }
}

object ImportanceStatsTest  {
  
  val NoTrees:Int = 1000
  val ZQuantiles = Map(1.0 -> 0.6826895, 2.0 ->  0.9544997, 3.0 -> 0.9973002, 4.0 -> 0.9999367)
  
}
