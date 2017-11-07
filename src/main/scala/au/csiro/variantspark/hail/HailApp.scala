package au.csiro.variantspark.hail

import is.hail.HailContext
import is.hail.expr._
import VSHailFunctions._
import au.csiro.variantspark.hail.adapter.HailFeatureSource
import org.apache.spark.sql.Row
import au.csiro.variantspark.hail.adapter.HailLabelSource
import au.csiro.variantspark.api.ImportanceAnalysis
import au.csiro.variantspark.algo.RandomForestParams


object HailApp {
  
  def main(args:Array[String]) = {
    println("Hello")
    val hc = HailContext()
    val table = hc.importTable("data/chr22-labels-hail.csv", keyNames = Array("sample"), separator=",", 
          types = Map("x22_16050408" -> TInt))
    println("Table signature: " + table.signature)
    
    val vcf = hc.importVCF("data/chr22_1000.vcf")   
    val annotatedVcfRaw =  vcf.annotateSamplesTable(table, root="sa.pheno")
    val annotatedVcf = annotatedVcfRaw.annotateSamplesExpr("sa.value = sa.pheno.x22_16050408")
    
    println("Samples schema annotated: " + annotatedVcf.saSignature)
       
    val c = annotatedVcf.xxx()
    println(c)    
    val fs = new HailFeatureSource(annotatedVcf)
    println(fs.sampleNames)
    fs.features().take(10).map(f => (f.label, f.values.toList)).foreach(println)
    val ls = new HailLabelSource(annotatedVcf, List("pheno", "x22_16050408"))
    val labels = ls.getLabels(fs.sampleNames)    
    println("Labels : " + labels.toList)
    
    val ia = new ImportanceAnalysis(hc.sqlContext, fs, ls, rfParams = RandomForestParams(), 
          nTrees = 500, rfBatchSize = 100, varOrdinalLevels = 3)
    
    ia.importantVariables(100).foreach(println)
  }
}