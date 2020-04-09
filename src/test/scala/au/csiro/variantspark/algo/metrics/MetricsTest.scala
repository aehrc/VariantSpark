package au.csiro.variantspark.algo.metrics

import org.junit.Assert._
import org.junit.Test

class MetricsTest {

  @Test
  def testCountAnyAlt() {
    val operation = AtLeastOneSharedAltAlleleCount
    assertEquals(0, operation.unitOp(0.toByte, 0.toByte))
    assertEquals(0, operation.unitOp(0.toByte, 1.toByte))
    assertEquals(0, operation.unitOp(1.toByte, 0.toByte))
    assertEquals(0, operation.unitOp(2.toByte, 0.toByte))
    assertEquals(0, operation.unitOp(0.toByte, 2.toByte))
    assertEquals(1, operation.unitOp(1.toByte, 1.toByte))
    assertEquals(1, operation.unitOp(1.toByte, 2.toByte))
    assertEquals(1, operation.unitOp(2.toByte, 1.toByte))
    assertEquals(1, operation.unitOp(2.toByte, 2.toByte))
  }

  @Test
  def testCountAllAlt() {
    val operation = SharedAltAlleleCount
    assertEquals(0, operation.unitOp(0.toByte, 0.toByte))
    assertEquals(0, operation.unitOp(0.toByte, 1.toByte))
    assertEquals(0, operation.unitOp(1.toByte, 0.toByte))
    assertEquals(0, operation.unitOp(2.toByte, 0.toByte))
    assertEquals(0, operation.unitOp(0.toByte, 2.toByte))
    assertEquals(1, operation.unitOp(1.toByte, 1.toByte))
    assertEquals(1, operation.unitOp(1.toByte, 2.toByte))
    assertEquals(1, operation.unitOp(2.toByte, 1.toByte))
    assertEquals(2, operation.unitOp(2.toByte, 2.toByte))
  }
}
