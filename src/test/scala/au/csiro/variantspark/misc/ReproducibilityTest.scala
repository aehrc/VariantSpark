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

/**
  * This test needs to be run standalone as it need a different spark context than other tests.
  */
class ReproducibilityTest extends SparkTest {

  override implicit lazy val spark = SparkSession.builder
    .config(new SparkConf(false))
    .appName("test")
    .master("local[*]")
    .getOrCreate()

  @Test
  def testReproducibleResults() {
    implicit val vsContext = VSContext(spark)
    implicit val sqlContext = spark.sqlContext
    val features = vsContext.importVCF("data/chr22_1000.vcf", 3)
    val label = vsContext.loadLabel("data/chr22-labels.csv", "22_16051249")
    val params = RandomForestParams(seed = 13L)
    val rfModel1 = RFModelTrainer.trainModel(features, label, params, 40, 20)
    val impAnalysis1 = new ImportanceAnalysis(sqlContext, features, rfModel1)
    val topVariables1 = impAnalysis1.importantVariables(20, false)
    val rfModel2 = RFModelTrainer.trainModel(features, label, params, 40, 20)
    val impAnalysis2 = new ImportanceAnalysis(sqlContext, features, rfModel2)
    val topVariables2 = impAnalysis2.importantVariables(20, false)
    topVariables1.zip(topVariables2).foreach { p => assertEquals(p._1, p._2) }
  }
}
