package au.csiro.variantspark.test.regression

import java.util.Collection

import scala.collection.JavaConverters.asJavaCollectionConverter

import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.runners.Parameterized.Parameters
import com.google.common.io.PatternFilenameFilter

/**
  * Runs regression test for syntetic datasets
  * The datasets are generated with `dev/test-get-synth-data.sh`
  */

@RunWith(classOf[Parameterized])
class ImportanceSynthRegressionTest(caseFile: String) extends ImportanceRegressionTest {
  import ImportanceSynthRegressionTest.caseFileRE

  @Test
  def testCaseImportanceOutputMatches() {
    caseFile match {
      case caseFileRE(prefix, _, _, ivo, _, response) =>
        runRegression(
          s"""importance -if ${synth(prefix)}-wide.csv -ff ${synth(prefix)}-labels.csv -fc ${response} -it csv -io {"defVariableType":"ORDINAL(${ivo})"} -v -rn 100 -rbs 50 -ro -sr 17 -on 100 -sp 4 -ovn to100 -of $${outputFile}""",
          caseFile)
    }
  }
}

object ImportanceSynthRegressionTest {
  import ImportanceRegressionTest._

  /**
    * Match test cases from such as:  synth_2000_500_fact_3_0.995-imp_cat2.csv
    */
  val caseFileRE = """(synth_([^_]+)_([^_]+)_fact_([^_]+)_([^_]+))-imp_([^_]+).csv""".r

  @Parameters
  def testCases: Collection[Array[Object]] =
    ExpectedDir
      .listFiles(new PatternFilenameFilter(caseFileRE.pattern))
      .map(f => Array[Object](f.getName))
      .toList
      .asJavaCollection
}
