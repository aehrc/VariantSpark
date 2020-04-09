package au.csiro.variantspark.test.regression

import org.junit.Assert._
import org.junit.Test
import au.csiro.variantspark.cli.VariantSparkApp
import org.apache.commons.io.FileUtils
import java.io.File
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.junit.BeforeClass
import org.apache.commons.lang3.text.StrSubstitutor
import collection.JavaConverters._

/**
  * Base class for regression test that compare importance output for know
  * datasets and parameters against the recorded one assumed to be correct.
  * The expected output can be updated with the `dev/test-get-regression-cases.sh`
  */
abstract class ImportanceRegressionTest {

  import ImportanceRegressionTest._
  def expected(fileName: String): String = new File(ExpectedDir, fileName).getPath
  def synth(fileName: String): String = new File(SynthDataDir, fileName).getPath
  def actual(fileName: String): String = new File(ActualDir, fileName).getPath

  def runRegression(cmdLine: String, expextedFileName: String,
      sessionBuilder: SparkSession.Builder = MasterLocal2) {
    withSessionBuilder(sessionBuilder) { _ =>
      val outputFile = actual(expextedFileName)
      val sub = new StrSubstitutor(Map("outputFile" -> outputFile).asJava)
      VariantSparkApp.main(sub.replace(cmdLine).split(" "))
      assertSameContent(expected(expextedFileName), outputFile)
    }
  }
}

object ImportanceRegressionTest {

  val ExpectedDir = new File("src/test/data/regression")
  val SynthDataDir = new File("src/test/data/synth")
  val ActualDir = new File("target/regression")

  val MasterLocal2 = SparkSession.builder().master("local[2]")

  //TODO: Refactor with AsertJ
  def assertSameContent(pathToExpectedFile: String, pathToActualFile: String) {
    assertEquals(FileUtils.readLines(new File(pathToExpectedFile)),
      FileUtils.readLines(new File(pathToActualFile)))
  }

  def withSessionBuilder(sessionBulider: SparkSession.Builder)(f: SparkSession => Unit) {
    var session: SparkSession = null
    try {
      session = sessionBulider.getOrCreate()
      f(session)
    } finally {
      if (session != null) {
        session.close()
      }
    }
  }

  @BeforeClass
  def setupClass() {
    ActualDir.mkdirs()
  }
}
