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
import org.junit.runner.RunWith
import org.junit.Ignore

object ImportanceRegressionTest {
  
  val ExpectedDir = new File("src/test/data/regression")
  val SynthDataDir = new File("src/test/data/synth")
  val ActualDir = new File("target/regression")
  
  val MasterLocal2 = SparkSession.builder().master("local[2]")

  //TODO: Refactor with AsertJ
  def assertSameContent(pathToExpectedFile: String, pathToActualFile: String) {
    assertEquals(FileUtils.readLines(new File(pathToExpectedFile)), FileUtils.readLines(new File(pathToActualFile)))
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

class ImportanceRegressionTest {

  import ImportanceRegressionTest._

  def expected(fileName:String):String  = new File(ExpectedDir, fileName).getPath
  def synth(fileName:String):String  = new File(SynthDataDir, fileName).getPath
  def actual(fileName:String):String  = new File(ActualDir, fileName).getPath
  
  //TODO: Refactor with ParametrizedTest: see: https://www.tutorialspoint.com/junit/junit_parameterized_test.htm
  def runRegression(cmdLine:String, expextedFileName:String, sessionBuilder:SparkSession.Builder = MasterLocal2) {
    withSessionBuilder(MasterLocal2) { _ =>
      val outputFile = actual(expextedFileName)
      val sub = new StrSubstitutor(Map("outputFile" -> outputFile).asJava)
      VariantSparkApp.main(sub.replace(cmdLine).split(" "))
      assertSameContent(expected(expextedFileName), outputFile)
    } 
  }
  
  def runSynthRegression(caseFile:String) {
    // synth_2000_500_fact_3_0.995-imp_cat2.csv
    val caseFileRE = """(synth_([^_]+)_([^_]+)_fact_([^_]+)_([^_]+))-imp_([^_]+).csv""".r
    caseFile match {
      case caseFileRE(prefix,_,_,ivo,_,response) => runRegression(s"importance -if ${synth(prefix)}-wide.csv -ff ${synth(prefix)}-labels.csv -fc ${response} -it csv -ivo ${ivo} -v -rn 100 -rbs 50 -ro -sr 17 -on 100 -sp 4 -of $${outputFile}",
          caseFile)
    }
  }
  
  @Test
  def testVFCImportance() {
    runRegression("importance -if data/chr22_1000.vcf -ff data/chr22-labels.csv -fc 22_16050408 -v -rn 100 -rbs 50 -ro -sr 17 -on 100 -ivb -sp 4 -of ${outputFile}",
        "chr22-imp_22_16050408.csv")
  }

  @Test
  def testCNAEImportance() {
    runRegression("importance -if data/CNAE-9-wide.csv -it csv -ff data/CNAE-9-labels.csv -fc category -v -ro -rn 100 -rbs 50 -sr 17 -ivo 10 -sp 4 -on 100 -of ${outputFile}",
        "CNAE-9-imp_category.csv")
  }  

  @Test
  def test_synth_2000_500_fact_3_0_995_imp_cat2() {
    runSynthRegression("synth_2000_500_fact_3_0.995-imp_cat2.csv")
  }  

  @Test
  def test_synth_2000_500_fact_3_0_995_imp_cat10() {
    runSynthRegression("synth_2000_500_fact_3_0.995-imp_cat10.csv")
  }  

  @Test
  def test_synth_2000_500_fact_3_0_imp_cat2() {
    runSynthRegression("synth_2000_500_fact_3_0.0-imp_cat2.csv")
  }  

  @Test
  def test_synth_2000_500_fact_3_0_imp_cat10() {
    runSynthRegression("synth_2000_500_fact_3_0.0-imp_cat10.csv")
  }  
  
  @Test
  def test_synth_2000_500_fact_10_0_995_imp_cat2() {
    runSynthRegression("synth_2000_500_fact_10_0.995-imp_cat2.csv")
  }  

  @Test
  def test_synth_2000_500_fact_10_0_995_imp_cat10() {
    runSynthRegression("synth_2000_500_fact_10_0.995-imp_cat10.csv")
  }  

  @Test
  def test_synth_2000_500_fact_10_0_imp_cat2() {
    runSynthRegression("synth_2000_500_fact_10_0.0-imp_cat2.csv")
  }  

  @Test
  def test_synth_2000_500_fact_10_0_imp_cat10() {
    runSynthRegression("synth_2000_500_fact_10_0.0-imp_cat10.csv")
  }  
}

  
