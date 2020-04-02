package au.csiro.variantspark.hail

import au.csiro.variantspark.test.SparkTest
import org.junit.Test
import org.junit.Assert._

import is.hail.HailContext
import is.hail.expr._
import au.csiro.variantspark.hail._
import au.csiro.variantspark.algo.metrics.ManhattanPairwiseMetric
import is.hail.expr.ir.IRParser
import au.csiro.variantspark.hail.methods.RFModel
import is.hail.table.Table
import is.hail.expr.ir.MatrixIR
import au.csiro.variantspark.test.TestSparkContext

object TestHailContext {
  lazy val hc = HailContext(TestSparkContext.spark.sparkContext)
}

class HailIntegrationTest extends SparkTest {
 
  val hc = TestHailContext.hc
  
  // This is textual version of the expression 
  // that is normally prouduced by python call to the API
  def loadDataToMatrixIr(vcfFilename:String, labelFilename:String, sampleName:String, 
      labelName:String, refGenome:String = "GRCh37"):String = s"""
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
                    (MatrixRead None False False "{\\"name\\":\\"MatrixVCFReader\\",\\"files\\":[\\"${vcfFilename}\\"],\\"callFields\\":[\\"PGT\\"],\\"entryFloatTypeName\\":\\"Float64\\",\\"rg\\":\\"${refGenome}\\",\\"contigRecoding\\":{},\\"arrayElementsRequired\\":true,\\"skipInvalidLoci\\":false,\\"gzAsBGZ\\":false,\\"forceGZ\\":false,\\"filterAndReplace\\":{\\"name\\":\\"TextInputFilterAndReplace\\"},\\"partitionsJSON\\":null}")
                    (TableKeyBy (${sampleName}) False
                      (TableRead None False "{\\"name\\":\\"TextTableReader\\",\\"options\\":{\\"files\\":[\\"${labelFilename}\\"],\\"typeMapStr\\":{\\"${labelName}\\":\\"Float64\\"},\\"comment\\":[],\\"separator\\":\\",\\",\\"missing\\":[\\"NA\\"],\\"noHeader\\":false,\\"impute\\":false,\\"quoteStr\\":null,\\"skipBlankLines\\":false,\\"forceBGZ\\":false,\\"filterAndReplace\\":{\\"name\\":\\"TextInputFilterAndReplace\\"},\\"forceGZ\\":false}}")))
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
                  (Apply nNonRefAlleles
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
        
  @Test
  def testRunImportanceAnalysisForGRCh37() {
    val strMatrixIR = loadDataToMatrixIr("data/chr22_1000.vcf", "data/chr22-labels-hail.csv", "sample", 
        "x22_16051480", "GRCh37")
    val matrixIR = IRParser.parse_matrix_ir(strMatrixIR)    
    val rfModel = RFModel.pyApply(matrixIR, None, true, None, None, Some(13))
    rfModel.fitTrees(100, 50)
    assertTrue("OOB Error is defined", !rfModel.oobError.isNaN) 
    val importanceTableValue = rfModel.variableImportance    
    val importanceTable = new Table(hc, importanceTableValue)    
    assertEquals(List("locus", "alleles", "importance"), importanceTable.signature.fieldNames.toList)
    assertEquals("All variables have reported importance", 1988, importanceTable.count())
    rfModel.release()
  } 
  
  @Test
  def testRunImportanceAnalysisForGRCh38() {
    val strMatrixIR = loadDataToMatrixIr("data/chr22_1000_GRCh38.vcf", "data/chr22-labels-hail.csv", "sample", 
        "x22_16051480", "GRCh38")
    val matrixIR = IRParser.parse_matrix_ir(strMatrixIR)    
    val rfModel = RFModel.pyApply(matrixIR, None, true, None, None, Some(13))
    rfModel.fitTrees(100, 50)
    assertTrue("OOB Error is defined", !rfModel.oobError.isNaN) 
    val importanceTableValue = rfModel.variableImportance    
    val importanceTable = new Table(hc, importanceTableValue)    
    assertEquals(List("locus", "alleles", "importance"), importanceTable.signature.fieldNames.toList)
    assertEquals("All variables have reported importance", 1988, importanceTable.count())
    rfModel.release()
  }
}