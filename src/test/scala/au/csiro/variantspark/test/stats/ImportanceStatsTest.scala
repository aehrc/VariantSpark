package au.csiro.variantspark.test.stats

import java.io.FileOutputStream

import au.csiro.pbdava.ssparkle.common.utils.CSVUtils
import au.csiro.variantspark.test.{AbstractCmdLineTest, TestCsvUtils, TestData}
import org.junit.Assert._
import org.junit.Test

/**
  * This test compare the importance measures produced by this implementation with ones generated with ranger.
  * The basic parameters are the same (the nunber or trees, mtry etc).
  * To account for randomness the comparison is made by simulating the distribution the importance of each variable
  * using ranger (by default with 50 reperitions) and estimating their means and standard deviations.
  *
  * To check if this implementation is correct we test if its importance results are within the normal quantiles.
  * E.g. that for about 68% of variables their importances are with 1 sd from the simulated mean, 95% within 2 x sd, etc.
  */
class ImportanceStatsTest extends AbstractCmdLineTest with TestData {

  import ImportanceStatsTest._

  def subdir: String = "stats"

  def quantile(data: Array[Double])(q: Double): Double = {
    data.filter(_ <= q).size.toDouble / data.size.toDouble
  }

  def runTest(caseName: String, labels: String) {
    val fullCaseName = s"${caseName}-${labels}"
    val labelsFile = data(s"${fullCaseName}.csv")
    val dataFile = data(s"${caseName}-wide.csv")
    val outputFile = actual(s"${caseName}-importance.csv")
    val nTrees = 2000

    runVariantSpark(
        s"""importance -if ${dataFile} -ff ${labelsFile} -fc cat2 -it csv -v -rn ${nTrees} -rbs 250 -sr 13 -on 0 -ovn raw -sp 4 -of ${outputFile}""")

    val importanceMeans = TestCsvUtils.readColumnToDoubleMap(s"${fullCaseName}-stats.csv", "mean")
    val importanceSds = TestCsvUtils.readColumnToDoubleMap(s"${fullCaseName}-stats.csv", "sd")
    val variableImportance = TestCsvUtils.readColumnToDoubleMap(outputFile, "importance")
    val residuals = variableImportance.map({
      case (k, v) => (k, (v - importanceMeans(k)) / importanceSds(k))
    })
    val residualsAbs = residuals.mapValues(Math.abs)

    CSVUtils.withStream(new FileOutputStream(actual(s"{fullCaseName}.csv"))) { writer =>
      val header = List("variable", "residual")
      writer.writeRow(header)
      writer.writeAll(residuals.toList.sortBy(_._1).map(_.productIterator.toSeq).toSeq)
    }

    val importanceQuantile = quantile(residualsAbs.values.toArray)(_)

    // Check that estimated quantiles are not much smaller than the normal ones.
    ZQuantiles.foreach {
      case (q, p) =>
        assertTrue(importanceQuantile(q) > p - 0.01)
    }

    // TODO: Add check on correlation (shoiuld be less than 0.2)
    // Use: org.apache.commons.math3.stat.correlation.PearsonsCorrelation
  }

  @Test
  def testImportanceWithSignal() {
    runTest("stats_100_1000_cont_0.0", "labels")
  }

  @Test
  def testImportanceWithNoSignal() {
    runTest("stats_100_1000_cont_0.0", "labels_null")
  }
}

object ImportanceStatsTest {

  val NoTrees: Int = 1000
  val ZQuantiles = Map(1.0 -> 0.6826895, 2.0 -> 0.9544997, 3.0 -> 0.9973002, 4.0 -> 0.9999367)

}
