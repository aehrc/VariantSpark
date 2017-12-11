package au.csiro.variantspark.hail

import au.csiro.variantspark.test.SparkTest
import org.junit.Test
import org.junit.Assert._

import is.hail.HailContext
import is.hail.expr._
import au.csiro.variantspark.hail._
import au.csiro.variantspark.algo.metrics.ManhattanPairwiseMetric

class HailIntegrationTest extends SparkTest {
 
  @Test
  def testRunImportanceAnalysis() {
    val hc = HailContext(sc)
    val table = hc.importTable("data/chr22-labels-hail.csv", keyNames = Array("sample"), separator=",")   
    val vcf = hc.importVCF("data/chr22_1000.vcf")   
    val annotatedVcf =  vcf.annotateSamplesTable(table, root="sa.pheno")
    val importnceAnalysis = annotatedVcf.importanceAnalysis("if (sa.pheno.x22_16051480=='1') 1 else 0")
    val importantVariables = importnceAnalysis.variantImportance(100)
    println(importantVariables.signature)
    importantVariables.collect().take(10).foreach(println)
  } 

  @Test
  def testRunsPaiwiseOperation() {
    val hc = HailContext(sc)
    val vcf = hc.importVCF("data/chr22_1000.vcf")   
    val pairwiseResult = vcf.pairwiseOperation(ManhattanPairwiseMetric)
    pairwiseResult.exportTSV("target/manhattan.tsv")
  } 

}