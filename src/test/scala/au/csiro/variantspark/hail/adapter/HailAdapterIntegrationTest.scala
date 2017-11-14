package au.csiro.variantspark.hail.adapter

import au.csiro.variantspark.test.SparkTest
import org.junit.Test
import org.junit.Assert._

import is.hail.HailContext
import is.hail.expr._
import au.csiro.variantspark.hail.adapter._
import au.csiro.variantspark.api.ImportanceAnalysis
import au.csiro.variantspark.algo.RandomForestParams


class HailAdapterIntegrationTest extends SparkTest {
  @Test
  def testRunImportanceAnalysis() {
    val hc = HailContext(sc)
    val table = hc.importTable("data/chr22-labels-hail.csv", keyNames = Array("sample"), separator=",")   
    val vcf = hc.importVCF("data/chr22_1000.vcf")   
    val annotatedVcf =  vcf.annotateSamplesTable(table, root="sa.pheno")
    val fs = HailFeatureSource(annotatedVcf)
    val ls = HailLabelSource(annotatedVcf,"if (sa.pheno.x22_16051480=='1') 1 else 0")
    implicit val vsContext = HailContextAdapter(hc)
    val ia = ImportanceAnalysis(fs, ls)
    val top10 = ia.importantVariables(10)
    assertTrue(top10.map(_._1).toSet.contains("22:16051480:T:C"))
  } 
}