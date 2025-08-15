package au.csiro.variantspark.api

import org.apache.spark.sql.functions._
import au.csiro.variantspark.test.SparkTest
import org.junit.Test
import org.junit.Assert._
import au.csiro.variantspark.api._
import au.csiro.variantspark.algo.RandomForestParams
import org.apache.spark.sql.{SparkSession, SQLContext}
import org.apache.spark.SparkConf

class ImportanceApiTest extends SparkTest {
  @Test
  def testImportanceAnalysisNewApi() {
    implicit val vsContext = VSContext(spark)
    implicit val sqlContext = spark.sqlContext
    val features = vsContext.importVCF("data/chr22_1000.vcf")
    val label = vsContext.loadLabel("data/chr22-labels.csv", "22_16050678")
    val params = RandomForestParams(seed = 17L)
    val rfModel = RFModelTrainer.trainModel(features, label, params, 200, 50)
    val impAnalysis = new ImportanceAnalysis(sqlContext, features, rfModel)
    val top10Variables = impAnalysis.importantVariables(10, normalized = true)
    top10Variables.foreach(println _)
    assertEquals(10, top10Variables.size)
    assertEquals("22_16050678_C_T", top10Variables.head._1)
  }
}
