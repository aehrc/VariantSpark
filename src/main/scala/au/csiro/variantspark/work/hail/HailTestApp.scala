package au.csiro.variantspark.work.hail

import is.hail.HailContext
import is.hail.expr._
import au.csiro.variantspark.hail.adapter.HailFeatureSource
import org.apache.spark.sql.Row
import is.hail.table.Table
import is.hail.expr.types.virtual.TInt64Required
import is.hail.io.vcf.VCFsReader
import is.hail.utils.TextInputFilterAndReplace
import is.hail.expr.ir.MatrixIR
import is.hail.expr.ir.MatrixLiteral
import is.hail.expr.ir.TableValue
import is.hail.expr.ir.TableLiteral

object HailTestApp {
  
  def main(args:Array[String]) = {
    println("Hello")
    val hc = HailContext()
  val table:Table = hc.importTable("data/chr22-labels-hail.csv", 
      keyNames = Some(Array("sample")), separator=",", types=Map("x22_16050408" -> TInt64Required)) 
  
  println(table)
  println("Signature: " + table.signature)

  val reader = new VCFsReader(
      Array("data/chr22_1000.vcf"),
      Set.empty,
      "Float64",
      None,
      Map.empty,
      true,
      true,
      true,
      false,
      TextInputFilterAndReplace(None, None, None),
      "[]", //partitionsJSON,
      None,
      None
   )

  val irArray = reader.read()
  
  println(irArray.toList)
  
  val matLit = irArray.head
  println(matLit)
   
  val matrixValue = matLit match { 
      case MatrixLiteral(v) => v  
  }
  
  println(matrixValue)
  
  println(matrixValue.stringSampleIds)
  
  val tableValue:TableValue = matrixValue.toTableValue
  println(tableValue)
  
  val colTable = new Table(table.hc, TableLiteral(tableValue))
  println(colTable.signature)
  //    
//  val vcf = hc.
  
  
//  .importVCF("data/chr22_1000.vcf")
//  println("Samples schema: " + vcf.saSignature)
//    
//    val annotatedVcf =  vcf.annotateSamplesTable(table, root="sa.pheno")
//    
//    println("Samples schema annotated: " + annotatedVcf.saSignature)
//    
//    val res2 = annotatedVcf.sampleIdsAndAnnotations.head._2
//    val res1 = annotatedVcf.sampleIdsAndAnnotations.head._1
//    //val res  = annotatedVcf.querySamples("samples.pheno.x22_16050408")
//    println(res1, res1.getClass())
//    println(res2, res2.getClass())
//    
//    val row  = res2.asInstanceOf[Row]
//    println("Row: " + row)
//    println(annotatedVcf.saSignature.query(List("pheno", "x22_16050408"))(row))
//    val fs = new HailFeatureSource(vcf)
//    println(fs.sampleNames)
//    fs.features.take(10).map(f => (f.label, f.valueAsStrings)).foreach(println)
//  
    }
}