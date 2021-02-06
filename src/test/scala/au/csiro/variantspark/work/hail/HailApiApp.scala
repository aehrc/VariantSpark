package au.csiro.variantspark.work.hail

import au.csiro.variantspark.hail.methods.RFModel
import is.hail.HailContext
import is.hail.backend.spark.SparkBackend
import is.hail.expr.ir.{Interpret, MatrixIR, TableIR}
import is.hail.utils.ExecutionTimer

/**
  * INFO: Simulates calling from python
  */
// scalastyle:off

object HailApiApp {

  //noinspection ScalaStyle
  def loadDataToMatrixIr(vcfFilename: String, labelFilename: String, sampleName: String,
      labelName: String): String = s"""
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
                    (MatrixRead None False False "{\\"name\\":\\"MatrixVCFReader\\",\\"files\\":[\\"${vcfFilename}\\"],\\"callFields\\":[\\"PGT\\"],\\"entryFloatTypeName\\":\\"Float64\\",\\"rg\\":\\"GRCh37\\",\\"contigRecoding\\":{},\\"arrayElementsRequired\\":true,\\"skipInvalidLoci\\":false,\\"gzAsBGZ\\":false,\\"forceGZ\\":false,\\"filterAndReplace\\":{\\"name\\":\\"TextInputFilterAndReplace\\"},\\"partitionsJSON\\":null}")
                    (TableKeyBy (${sampleName}) False
                      (TableRead None False "{\\"name\\":\\"TextTableReader\\",\\"files\\":[\\"${labelFilename}\\"],\\"typeMapStr\\":{\\"${labelName}\\":\\"Float64\\"},\\"comment\\":[],\\"separator\\":\\",\\",\\"missing\\":[\\"NA\\"],\\"hasHeader\\":true,\\"impute\\":false,\\"quoteStr\\":null,\\"skipBlankLines\\":false,\\"forceBGZ\\":false,\\"filterAndReplace\\":{\\"name\\":\\"TextInputFilterAndReplace\\"},\\"forceGZ\\":false}")))
                  (InsertFields
                    (SelectFields (s)
                      (Ref sa))
                    None
                    (${labelName}
                      (GetField __uid_3
                        (Ref sa)))))
                (InsertFields
                  (SelectFields (s ${labelName})
                    (Ref sa))
                  None
                  (__uid_4
                    (GetField ${labelName}
                      (GetField ${labelName}
                        (Ref sa))))
                  (__uid_5
                    (F64 1.0))))
              (InsertFields
                (SelectFields (GT)
                  (Ref g))
                None
                (__uid_6
                  (Apply nNonRefAlleles () Int32
                    (GetField GT
                      (Ref g))))))
            (SelectFields (s ${labelName} __uid_4 __uid_5)
              (Ref sa)))
          (SelectFields (${labelName} __uid_4 __uid_5)
            (SelectFields (s ${labelName} __uid_4 __uid_5)
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
        (SelectFields (${labelName} __uid_4 __uid_5)
          (Ref sa))))
    (SelectFields (__uid_6)
      (SelectFields (GT __uid_6)
        (Ref g)))))
  """

  def main(args: Array[String]): Unit = {
    println("Hello")

    val sparkBackend = SparkBackend.getOrCreate();
    val hc = HailContext.getOrCreate(sparkBackend)

    val matrixExpr = loadDataToMatrixIr("data/hipsterIndex/hipster.vcf.bgz",
      "data/hipsterIndex/hipster_labels.txt", "samples", "label")
    println(matrixExpr)

//    val matrixIR =
//      sparkBackend.parse_matrix_ir(matrixExpr, Collections.emptyMap(), Collections.emptyMap())
    val matrixIR = MatrixIR.read(sparkBackend.fs, "tmp/chr22_1000_selected.vds")
    println(matrixIR)

    println(matrixIR.typ)
    println(matrixIR.typ.rowKeyStruct)
    println(matrixIR.typ.rowKey)
    println(matrixIR.typ.colKey)

    val rfModel =
      RFModel.pyApply(sparkBackend, matrixIR, null, true, null, null, null, "mode")
    rfModel.fitTrees(100, 50)
    println(s"OOB Error  = ${rfModel.oobError}")
    val importanceTableValue: TableIR = rfModel.variableImportance

    val r = ExecutionTimer.logTime("HailApiApp.collectValues") { timer =>
      sparkBackend.withExecuteContext(timer) { ctx =>
        val tv = Interpret.apply(importanceTableValue, ctx, true)
        tv.rdd.collect().take(10).foreach(println)
      }
    }

    val rangeIR = MatrixIR.range(10, 20, None)
    println(rangeIR.typ)

  }
}
// scalastyle:on
