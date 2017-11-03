package au.csiro.variantspark.hail

import is.hail.HailContext
import is.hail.expr._
import VSHailFunctions._
import au.csiro.variantspark.hail.adapter.HailFeatureSource
import org.apache.spark.sql.Row


object HailTestApp {
  
  def main(args:Array[String]) = {
    println("Hello")
    val hc = HailContext()

    val table = hc.importTable("data/chr22-labels-hail.csv", keyNames = Array("sample"), separator=",", 
          types = Map("x22_16050408" -> TInt))
    println(table)
    
    println("Signature: " + table.signature)
    
    val vcf = hc.importVCF("data/chr22_1000.vcf")
    println("Samples schema: " + vcf.saSignature)
    
    val annotatedVcf =  vcf.annotateSamplesTable(table, root="sa.pheno")
    
    println("Samples schema annotated: " + annotatedVcf.saSignature)
    
    val res2 = annotatedVcf.sampleIdsAndAnnotations.head._2
    val res1 = annotatedVcf.sampleIdsAndAnnotations.head._1
    //val res  = annotatedVcf.querySamples("samples.pheno.x22_16050408")
    println(res1, res1.getClass())
    println(res2, res2.getClass())
    
    val row  = res2.asInstanceOf[Row]
    println("Row: " + row)
    println(annotatedVcf.saSignature.query(List("pheno", "x22_16050408"))(row))
    
    
    val c = vcf.xxx()
    println(c)    
    val fs = new HailFeatureSource(vcf)
    println(fs.sampleNames)
    fs.features().take(10).map(f => (f.label, f.values.toList)).foreach(println)

  }
}