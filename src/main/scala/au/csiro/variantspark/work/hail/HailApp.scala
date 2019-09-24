package au.csiro.variantspark.work.hail

import is.hail.HailContext
import is.hail.expr._
import au.csiro.variantspark.api.ImportanceAnalysis
import org.apache.spark.sql.functions.udf
import is.hail.variant.Locus
import org.apache.spark.sql.Row

object HailApp {
  
//  def toLocus(label:String):Locus = {
//    val contigAndPosition = label.split("_")
//    Locus(contigAndPosition(0),contigAndPosition(1).toInt)
//  }
//  
//  def main(args:Array[String]) = {
//    println("Hello")
//    val hc = HailContext()
//    val table = hc.importTable("data/chr22-labels-hail.csv", keyNames = Array("sample"), separator=",", 
//          types = Map("x22_16050408" -> TInt))
//    println("Table signature: " + table.signature)
//    
//    val vcf = hc.importVCF("data/chr22_1000.vcf")   
//    val annotatedVcfRaw =  vcf.annotateSamplesTable(table, root="sa.pheno")
//    val annotatedVcf = annotatedVcfRaw.annotateSamplesExpr("sa.value = sa.pheno.x22_16050408")
//    
//    println("Samples schema annotated: " + annotatedVcf.saSignature)
//       
//    val fs = HailFeatureSource(annotatedVcf)
//    println(fs.sampleNames)
//    fs.features.take(10).map(f => (f.label, f.valueAsStrings)).foreach(println)
//    val ls = HailLabelSource(annotatedVcf,"if (sa.pheno.x22_16051480=='1') 1 else 0")
//    val labels = ls.getLabels(fs.sampleNames)    
//    println("Labels : " + labels.toList)
//    implicit val vsContext = HailContextAdapter(hc)
//    val ia = ImportanceAnalysis(fs, ls)
//    ia.importantVariables(100).foreach(println)
//    val df = ia.variableImportance
//    
//    val impSignature = TStruct(("locus", TLocus), ("importance",TDouble))
//    
//    val importanceKeyTable = KeyTable.apply(hc, df.rdd.map(r =>  
//      Row(toLocus(r.getString(0)), r.getDouble(1))), impSignature, Array("locus"))
//    val xxx = annotatedVcf.annotateVariantsTable(importanceKeyTable, null, "va.gini")
//    println(xxx.vaSignature)
//  }
}