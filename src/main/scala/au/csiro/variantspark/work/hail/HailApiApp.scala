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
import is.hail.expr.{ir, _}
import is.hail.expr.ir._
import is.hail.expr.types.virtual.TFloat64
import is.hail.expr.types.virtual._
import is.hail.expr.types.virtual.TStruct
import is.hail.methods.LinearRegressionRowsSingle
import au.csiro.variantspark.hail.methods.RFModel



/**
 * INFO: Simulates calling from python
 */
object HailApiApp {
  
  def main(args:Array[String]) = {
    println("Hello")
    val hc = HailContext()
 
    val matrixExpr = """
(MatrixRename () () ("__uid_4" "__uid_5") ("y" "z") () () ("__uid_6") ("e")     
  (MatrixMapEntries
    (MatrixMapCols None
      (MatrixMapRows
        (MatrixMapCols None
          (MatrixMapCols ()
            (MatrixMapEntries
              (MatrixMapCols None
                (MatrixMapCols None
                  (MatrixAnnotateColsTable "__uid_3"
                    (MatrixRead None False False "{\"name\":\"MatrixVCFReader\",\"files\":[\"data/hipsterIndex/hipster.vcf.bgz\"],\"callFields\":[\"PGT\"],\"entryFloatTypeName\":\"Float64\",\"rg\":\"GRCh37\",\"contigRecoding\":{},\"arrayElementsRequired\":true,\"skipInvalidLoci\":false,\"gzAsBGZ\":false,\"forceGZ\":false,\"filterAndReplace\":{\"name\":\"TextInputFilterAndReplace\"},\"partitionsJSON\":null}")
                    (TableKeyBy (samples) False
                      (TableRead None False "{\"name\":\"TextTableReader\",\"options\":{\"files\":[\"data/hipsterIndex/hipster_labels.txt\"],\"typeMapStr\":{\"label\":\"Float64\",\"score\":\"Float64\"},\"comment\":[],\"separator\":\",\",\"missing\":[\"NA\"],\"noHeader\":false,\"impute\":false,\"quoteStr\":null,\"skipBlankLines\":false,\"forceBGZ\":false,\"filterAndReplace\":{\"name\":\"TextInputFilterAndReplace\"},\"forceGZ\":false}}")))
                  (InsertFields
                    (SelectFields (s)
                      (Ref sa))
                    None
                    (label
                      (GetField __uid_3
                        (Ref sa)))))
                (InsertFields
                  (SelectFields (s label)
                    (Ref sa))
                  None
                  (__uid_4
                    (GetField label
                      (GetField label
                        (Ref sa))))
                  (__uid_5
                    (F64 1.0))))
              (InsertFields
                (SelectFields (GT)
                  (Ref g))
                None
                (__uid_6
                  (Apply nNonRefAlleles
                    (GetField GT
                      (Ref g))))))
            (SelectFields (s label __uid_4 __uid_5)
              (Ref sa)))
          (SelectFields (label __uid_4 __uid_5)
            (SelectFields (s label __uid_4 __uid_5)
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
      (SelectFields (__uid_4 __uid_5)
        (SelectFields (label __uid_4 __uid_5)
          (Ref sa))))
    (SelectFields (__uid_6)
      (SelectFields (GT __uid_6)
        (Ref g)))))
  """
  
    val matrixIR = IRParser.parse_matrix_ir(matrixExpr)
    println(matrixIR)
  
    println(matrixIR.typ.rowKeyStruct)
    println(matrixIR.typ.rowKey)
    
    
    val rfModel = RFModel.pyApply(matrixIR)
    rfModel.fitTrees(100, 50)
    println(s"OOB Error  = ${rfModel.oobError}")    
    val importanceTableValue = rfModel.variableImportance
    
    val importanceTable = new Table(hc, importanceTableValue)
    println(importanceTable.signature)
    importanceTable.collect().take(10).foreach(println _)
  }
}