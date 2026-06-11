package au.csiro.variantspark.misc

import org.apache.spark.sql.functions._
import au.csiro.variantspark.test.SparkTest
import org.junit.Test
import org.junit.Ignore
import org.junit.Assert._
import au.csiro.variantspark.api._
import au.csiro.variantspark.algo.RandomForestParams
import org.apache.spark.sql.{SparkSession, SQLContext}
import org.apache.spark.SparkConf
import java.util.{Arrays, ArrayList}
import scala.jdk.CollectionConverters._

/**
  * This test needs to be run standalone as it need a different spark context than other tests.
  */
class CovariateReproducibilityTest extends SparkTest {

  override implicit lazy val spark = SparkSession.builder
    .config(new SparkConf(false))
    .appName("test")
    .master("local[*]")
    .getOrCreate()

  @Ignore
  @Test
  def testCovariateReproducibleResults() {
    implicit val vsContext = VSContext(spark)
    implicit val sqlContext = spark.sqlContext
    val genotypes = vsContext.importVCF("data/chr22_1000.vcf")
    val optVariableTypes = new ArrayList[String](Arrays.asList("CONTINUOUS", "ORDINAL(2)",
        "CONTINUOUS", "CONTINUOUS", "CONTINUOUS", "CONTINUOUS"))
    val covariates =
      vsContext.importStdCSV("data/chr22_1000_full_pheno.csv", optVariableTypes)
    val label = vsContext.loadResponse("data/chr22-labels.csv", "22_16051249")
    val features = vsContext.unionFeatureSources(genotypes, covariates)
    val params = RandomForestParams(seed = 13L)
    val trainResult1 = RFModelTrainer.trainModel(features, label, params, 40, 20)
    val impAnalysis1 =
      new ImportanceAnalysis(sqlContext, trainResult1.model, trainResult1.indexedFeatures)
    val topVariables1 = impAnalysis1.importantVariables(20, false)
    trainResult1.releaseIndexedData()
    val trainResult2 = RFModelTrainer.trainModel(features, label, params, 40, 20)
    val impAnalysis2 =
      new ImportanceAnalysis(sqlContext, trainResult2.model, trainResult2.indexedFeatures)
    val topVariables2 = impAnalysis2.importantVariables(20, false)
    trainResult2.releaseIndexedData()
    topVariables1.zip(topVariables2).foreach { p => assertEquals(p._1, p._2) }
  }
}
