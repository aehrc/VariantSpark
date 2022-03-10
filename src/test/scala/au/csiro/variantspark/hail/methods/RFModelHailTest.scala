package au.csiro.variantspark.hail.methods

import au.csiro.pbdava.ssparkle.common.utils.LoanUtils.withResource
import au.csiro.variantspark.test.TestSparkContext
import is.hail.HailContext
import is.hail.backend.spark.SparkBackend
import is.hail.expr.ir.{Interpret, MatrixIR, TableIR}
import is.hail.utils.ExecutionTimer
import org.junit.Assert._
import org.junit.Test

class RFModelHailTest {

  val sparkBackend = SparkBackend.getOrCreate(sc = TestSparkContext.spark.sparkContext);
  val hc = HailContext.getOrCreate(sparkBackend, skipLoggingConfiguration = true)

  /**
    * The purpose of this test is basic valdation of the hail interface
    * More substantial testing of the results is done from python
    * The input vds for this test is created by:
    *  `src/test/python/extract_rf_vds.py` called from `dev/test-gen-hail-cases.sh`
    */
  @Test
  def testRunImportanceAnalysisWithMissingCalls() {

    val matrixIR =
      MatrixIR.read(sparkBackend.fs, "src/test/data/hail/chr22_1000_missing-22_16050408.vds")

    withResource(RFModel.pyApply(sparkBackend, matrixIR, null, true, null, null, 13, "mode")) {
      rfModel =>
        rfModel.fitTrees(100, 50)
        assertTrue("OOB Error is defined", !rfModel.oobError.isNaN)
        val importanceTableIR: TableIR = rfModel.variableImportance

        assertEquals(List("locus", "alleles", "importance", "splitCount"),
          importanceTableIR.typ.rowType.fieldNames.toList)

        // collect the valued form the TableIR
        // they cannot be directly converted to DataFrame as the contain
        // non SparkSQL types such as Locus, which needs to be converted to structs
        // before conversion to spark DataFame is possible
        val collectedRows = ExecutionTimer.logTime("HailApiApp.collectValues") { timer =>
          sparkBackend.withExecuteContext(timer) { ctx =>
            val tv = Interpret(importanceTableIR, ctx, true)
            tv.rvd.collect(ctx)
          }
        }
        // Each row is [Locus, GenericArray, Double]
        assertEquals("All variables have reported importance", 1988, collectedRows.size)
    }
  }

  /**
   * The purpose of this test is basic valdation of the hail interface
   * More substantial testing of the results is done from python
   * The input vds for this test is created by:
   *  `src/test/python/extract_rf_pheno_vds.py` called from `dev/test-gen-hail-cases.sh`
   */
  @Test
  def testRunImportanceAnalysisWithCovriates() {

    val matrixIR =
      MatrixIR.read(sparkBackend.fs, "src/test/data/hail/chr22_1000-22_16050408-pheno.vds")

    withResource(RFModel.pyApply(sparkBackend, matrixIR, null, true, null, null, 13, "mode")) {
      rfModel =>
        rfModel.fitTrees(100, 50)
        assertTrue("OOB Error is defined", !rfModel.oobError.isNaN)
        val importanceTableIR: TableIR = rfModel.variableImportance

        assertEquals(List("locus", "alleles", "importance", "splitCount"),
          importanceTableIR.typ.rowType.fieldNames.toList)

        // collect the valued form the TableIR
        // they cannot be directly converted to DataFrame as the contain
        // non SparkSQL types such as Locus, which needs to be converted to structs
        // before conversion to spark DataFame is possible
        val collectedRows = ExecutionTimer.logTime("HailApiApp.collectValues") { timer =>
          sparkBackend.withExecuteContext(timer) { ctx =>
            val tv = Interpret(importanceTableIR, ctx, true)
            tv.rvd.collect(ctx)
          }
        }
        // Each row is [Locus, GenericArray, Double]
        assertEquals("All variables have reported importance", 1988, collectedRows.size)
    }
  }


}
