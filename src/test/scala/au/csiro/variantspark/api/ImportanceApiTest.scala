package au.csiro.variantspark.api

import org.apache.spark.sql.functions._
import au.csiro.variantspark.test.SparkTest
import org.junit.Test
import org.junit.Assert._
import au.csiro.variantspark.api._

class ImportanceApiTest extends SparkTest {
  @Test
  def testImportanceAnalysisLegacyApi() {
    implicit val vsContext = VSContext(spark)
    val fs = vsContext.featureSource("data/chr22_1000.vcf")
    assertEquals(1092, fs.sampleNames.size)
    val ls = vsContext.loadLabel("data/chr22-labels.csv", "22_16051347")
    assertEquals(1092, ls.getLabels(fs.sampleNames).length)
    val importanceAnalysis = ImportanceAnalysis(fs, ls, nTrees = 100, seed = Some(13L))
    val importanceDF = importanceAnalysis.variableImportance
    import importanceDF.sqlContext._
    importanceDF.cache()
    val top10Variables = importanceDF.orderBy(desc("importance")).limit(10).collect()
    assertEquals(10, top10Variables.size)
    assertEquals("22_16051347_G_C", top10Variables.head.getString(0))
  }
  
  @Test
  def testImportanceAnalysisNewApi() {    
    implicit val vsContext = VSContext(spark)
    val features = vsContext.importVCF("data/chr22_1000.vcf")
    val label = vsContext.loadLabel("data/chr22-labels.csv", "22_16051347")
    val impAnalysis = features.importanceAnalysis(label, nTrees = 100, seed = Some(13L))
    val top10Variables = impAnalysis.importantVariables(10)
    assertEquals(10, top10Variables.size)
    assertEquals("22_16051347_G_C", top10Variables.head._1)
  }
}
