package au.csiro.variantspark.work.hail

import is.hail.HailContext
import is.hail.expr._
import org.apache.spark.sql.Row
import is.hail.table.Table
import is.hail.expr.types.virtual.TInt64Required
import is.hail.io.vcf.VCFsReader
import is.hail.utils.TextInputFilterAndReplace
import is.hail.expr.ir.MatrixIR
import is.hail.expr.ir.MatrixLiteral
import is.hail.expr.ir.TableValue
import is.hail.expr.ir.TableLiteral
import is.hail.expr.{ir, _}
import is.hail.expr.ir._
import is.hail.expr.types.virtual.TFloat64
import is.hail.expr.types.virtual._
import is.hail.expr.types.virtual.TStruct
import is.hail.methods.LinearRegressionRowsSingle

object HailTestApp {

  def main(args: Array[String]) = {
    println("Hello")

    val hc = HailContext()

    val expr =
      """
	(TableCount
	  (MatrixColsTable
	    (MatrixMapCols None
	      (MatrixAnnotateColsTable "__uid_44"
	        (MatrixRead None False False "{\"name\":\"MatrixVCFReader\",\"files\":[\"data/hipsterIndex/hipster.vcf.bgz\"],\"callFields\":[\"PGT\"],\"entryFloatTypeName\":\"Float64\",\"rg\":\"GRCh37\",\"contigRecoding\":{},\"arrayElementsRequired\":true,\"skipInvalidLoci\":false,\"gzAsBGZ\":false,\"forceGZ\":false,\"filterAndReplace\":{\"name\":\"TextInputFilterAndReplace\"},\"partitionsJSON\":null}")
	        (TableKeyBy (samples) False
	          (TableRead None False "{\"name\":\"TextTableReader\",\"options\":{\"files\":[\"data/hipsterIndex/hipster_labels.txt\"],\"typeMapStr\":{\"label\":\"Int64\",\"score\":\"Float64\"},\"comment\":[],\"separator\":\",\",\"missing\":[\"NA\"],\"noHeader\":false,\"impute\":false,\"quoteStr\":null,\"skipBlankLines\":false,\"forceBGZ\":false,\"filterAndReplace\":{\"name\":\"TextInputFilterAndReplace\"},\"forceGZ\":false}}")))
	      (InsertFields
	        (SelectFields (s)
	          (Ref sa))
	        None
	        (label
	          (GetField __uid_44
	            (Ref sa)))))))
  """

    val ir = IRParser.parse_value_ir(expr)
    println(ir)

    val lrIRString =
      """
  	(TableMapRows
	  (MatrixToTableApply "{\"name\":\"LinearRegressionRowsSingle\",\"yFields\":[\"__y_0\"],\"xField\":\"__uid_102\",\"covFields\":[\"__cov0\"],\"rowBlockSize\":16,\"passThrough\":[]}"
	    (MatrixRename () () ("__uid_103" "__uid_104") ("__y_0" "__cov0") () () ("__uid_105") ("__uid_102")
	      (MatrixMapEntries
	        (MatrixMapCols None
	          (MatrixMapRows
	            (MatrixMapCols None
	              (MatrixMapCols ()
	                (MatrixMapEntries
	                  (MatrixMapCols None
	                    (MatrixMapCols None
	                      (MatrixAnnotateColsTable "__uid_44"
	                        (MatrixRead None False False "{\"name\":\"MatrixVCFReader\",\"files\":[\"data/hipsterIndex/hipster.vcf.bgz\"],\"callFields\":[\"PGT\"],\"entryFloatTypeName\":\"Float64\",\"rg\":\"GRCh37\",\"contigRecoding\":{},\"arrayElementsRequired\":true,\"skipInvalidLoci\":false,\"gzAsBGZ\":false,\"forceGZ\":false,\"filterAndReplace\":{\"name\":\"TextInputFilterAndReplace\"},\"partitionsJSON\":null}")
	                        (TableKeyBy (samples) False
	                          (TableRead None False "{\"name\":\"TextTableReader\",\"options\":{\"files\":[\"data/hipsterIndex/hipster_labels.txt\"],\"typeMapStr\":{\"label\":\"Int64\",\"score\":\"Float64\"},\"comment\":[],\"separator\":\",\",\"missing\":[\"NA\"],\"noHeader\":false,\"impute\":false,\"quoteStr\":null,\"skipBlankLines\":false,\"forceBGZ\":false,\"filterAndReplace\":{\"name\":\"TextInputFilterAndReplace\"},\"forceGZ\":false}}")))
	                      (InsertFields
	                        (SelectFields (s)
	                          (Ref sa))
	                        None
	                        (label
	                          (GetField __uid_44
	                            (Ref sa)))))
	                    (InsertFields
	                      (SelectFields (s label)
	                        (Ref sa))
	                      None
	                      (__uid_103
	                        (GetField score
	                          (GetField label
	                            (Ref sa))))
	                      (__uid_104
	                        (F64 1.0))))
	                  (InsertFields
	                    (SelectFields (GT)
	                      (Ref g))
	                    None
	                    (__uid_105
	                      (ApplyIR toFloat64
	                        (Apply nNonRefAlleles
	                          (GetField GT
	                            (Ref g)))))))
	                (SelectFields (s label __uid_103 __uid_104)
	                  (Ref sa)))
	              (SelectFields (label __uid_103 __uid_104)
	                (SelectFields (s label __uid_103 __uid_104)
	                  (Ref sa))))
	            (SelectFields (locus alleles)
	              (MakeStruct
	                (locus
	                  (GetField locus
	                    (Ref va)))
	                (alleles
	                  (GetField alleles
	                    (Ref va)))
	                (rsid
	                  (GetField rsid
	                    (Ref va)))
	                (qual
	                  (GetField qual
	                    (Ref va)))
	                (filters
	                  (GetField filters
	                    (Ref va)))
	                (info
	                  (GetField info
	                    (Ref va))))))
	          (SelectFields (__uid_103 __uid_104)
	            (SelectFields (label __uid_103 __uid_104)
	              (Ref sa))))
	        (SelectFields (__uid_105)
	          (SelectFields (GT __uid_105)
	            (Ref g))))))
	  (InsertFields
	    (SelectFields (locus alleles n sum_x y_transpose_x beta standard_error t_stat p_value)
	      (Ref row))
	    None
	    (y_transpose_x
	      (ApplyIR indexArray
	        (GetField y_transpose_x
	          (Ref row))
	        (I32 0)))
	    (beta
	      (ApplyIR indexArray
	        (GetField beta
	          (Ref row))
	        (I32 0)))
	    (standard_error
	      (ApplyIR indexArray
	        (GetField standard_error
	          (Ref row))
	        (I32 0)))
	    (t_stat
	      (ApplyIR indexArray
	        (GetField t_stat
	          (Ref row))
	        (I32 0)))
	    (p_value
	      (ApplyIR indexArray
	        (GetField p_value
	          (Ref row))
	        (I32 0)))))
"""

    val lrIR = IRParser.parse_table_ir(lrIRString)
    println(lrIR)

    val lrResult = new Table(hc, lrIR)

    println("#### KKey : " + lrResult.key)

    //println("Result lr count: " + lrResult.count())
    println(lrResult.signature)
    lrResult.collect().take(10).foreach(println _)

    //System.exit(0)

    val table: Table =
      hc.importTable("data/hipsterIndex/hipster_labels.txt", keyNames = Some(Array("samples")),
        separator = ",", types = Map("label" -> TInt64Required, "score" -> TFloat64Required))

    println("Table tir: " + table.tir)

    println(table)
    println("Signature: " + table.signature)

    val reader = new VCFsReader(Array("data/hipsterIndex/hipster.vcf.bgz"), Set.empty, "Float64",
      None, Map.empty, true, true, true, false, TextInputFilterAndReplace(None, None, None),
      "[]", //partitionsJSON,
      None, None)

    val irArray = reader.read()

    println(irArray.toList)

    val matLit = irArray.head
    println(matLit)

    val matrixValue = matLit match {
      case MatrixLiteral(v) => v
    }

    println(matrixValue)

    val tv = matrixValue.typ.rowKey

    println("XXXXXX XXXXXX")

    println(tv)
    println(matrixValue.typ.rowType)
    println(matrixValue.typ.rowKeyStruct)
    println(matrixValue.typ.rowValueStruct)
    println(matrixValue.typ.rowValueFieldIdx)

    System.exit(0)
//
//  println(matrixValue.stringSampleIds)
//  val tableValue:TableValue = matrixValue.toTableValue
//  println(tableValue)
//
//  val colTable = new Table(table.hc, TableLiteral(tableValue))
//  println(colTable.signature)  //
////  val vcf = hc.
//
//  val count = table.count()
//  println(s"Count: ${count}")

    val joinded_type = TStruct(("s", TStringOptional),
      ("_xxx", TStruct(("score", TFloat64Optional), ("label", TInt64Optional))))

    val matAnnot = MatrixAnnotateColsTable(matLit, table.tir, "label")
    println(matAnnot.typ.colType)

//  val exptr = //TableCount(
//    MatrixColsTable(
//        MatrixMapCols(
//          matAnnot,
//          InsertFields(
//              SelectFields(Ref("sa", TStruct(("s", TStringOptional))), Seq("s")),
//              Seq(("label", GetField(Ref("sa", joinded_type) , "_xxx")))
//          ),
//          None
//         )
//      )

    val exptr = //TableCount(
      MatrixColsTable(matAnnot)

    //val result = ir.Interpret[Long](exptr)
    //println(result)

    val colTable = new Table(table.hc, exptr)
    println(colTable.signature) //
    colTable.collect().take(10).foreach(println _)

    val lr = LinearRegressionRowsSingle(Seq("label.score"), "GT.n_alt_alleles()", Seq.empty, 1,
      Seq.empty)

    val tableIR = MatrixToTableApply(matAnnot, lr)
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
