package au.csiro.variantspark.work.hail

import is.hail.HailContext
import is.hail.expr._
import au.csiro.variantspark.api.ImportanceAnalysis
import au.csiro.variantspark.algo.RandomForestParams


object HailHipsterApp {
  
//  def main(args:Array[String]) = {
//    println("Hello")
//    val hc = HailContext()
//    val table = hc.importTable("data/hipsterIndex/hipster_labels.txt", keyNames = Array("samples"), separator=",", 
//          types = Map("label" -> TInt, "score" -> TDouble))
//    println("Table signature: " + table.signature)
//    
//    val vcf = hc.importVCF("data/hipsterIndex/hipster.vcf.bgz", nPartitions = Some(8))   
//    val annotatedVcfRaw =  vcf.annotateSamplesTable(table, root="sa.pheno")
//    val annotatedVcf = annotatedVcfRaw.annotateSamplesExpr("sa.value = sa.pheno.label")
//    
//    println("Samples schema annotated: " + annotatedVcf.saSignature)
//       
//    val fs = new HailFeatureSource(annotatedVcf)
//    println(fs.sampleNames)
//    fs.features.take(10).map(f => (f.label, f.valueAsStrings)).foreach(println)
//    val ls = new HailLabelSource(annotatedVcf, "sa.pheno.label")
//    val labels = ls.getLabels(fs.sampleNames)    
//    println("Labels : " + labels.toList)
//    
//    val ia = new ImportanceAnalysis(hc.sqlContext, fs, ls, rfParams = RandomForestParams(), 
//          nTrees = 500, rfBatchSize = 100, varOrdinalLevels = 3)
//    
//    ia.importantVariables(100).foreach(println)
//  }
}