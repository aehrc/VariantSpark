package au.csiro.variantspark.test.regression

import java.util.Collection

import scala.collection.JavaConverters.asJavaCollectionConverter

import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.runners.Parameterized.Parameters


/**
 * Runs regression test for real world datasets 
 */
@RunWith(classOf[Parameterized])
class ImportanceDatasetRegressionTest(filenameWithExpected:String, cmdLine:String) extends ImportanceRegressionTest {

  @Test
  def testDatasetImportanceOutputMatches() {
    runRegression(cmdLine, filenameWithExpected)
  }
}

object ImportanceDatasetRegressionTest {
  
  @Parameters
  def datasets():Collection[Array[Object]] = List(
        Array[Object]("chr22-imp_22_16050408.csv", "importance -if data/chr22_1000.vcf -ff data/chr22-labels.csv -fc 22_16050408 -v -rn 100 -rbs 50 -ro -sr 17 -on 100 -sp 4 -of ${outputFile} -ovn to100"),
        Array[Object]("CNAE-9-imp_category.csv", """importance -if data/CNAE-9-wide.csv -it csv -ff data/CNAE-9-labels.csv -fc category -v -ro -rn 100 -rbs 50 -sr 17 -io {"defVariableType":"ORDINAL(10)"} -sp 4 -on 100 -of ${outputFile} -ovn to100""")
      ).asJavaCollection
}