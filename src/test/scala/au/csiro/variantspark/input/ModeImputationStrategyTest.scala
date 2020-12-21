package au.csiro.variantspark.input

import au.csiro.variantspark.input.Missing.BYTE_NA_VALUE
import org.junit.Assert._
import org.junit.Test;

class ModeImputationStrategyTest {

  @Test
  def imputesEmptyArrayCorrectly() {
    assertArrayEquals(Array.emptyByteArray,
      ModeImputationStrategy(1).impute(Array.emptyByteArray))
  }

  @Test
  def imputesAllMissingToZeros {
    assertArrayEquals(Array.fill(3)(0.toByte),
      ModeImputationStrategy(1).impute(Array.fill(3)(BYTE_NA_VALUE)))
  }
  @Test
  def imputesMissingToTheMode {
    assertArrayEquals(Array(1.toByte, 1.toByte, 1.toByte, 0.toByte, 1.toByte, 1.toByte),
      ModeImputationStrategy(3).impute(Array(BYTE_NA_VALUE, BYTE_NA_VALUE, BYTE_NA_VALUE, 0.toByte, 1.toByte, 1.toByte)))
  }

  @Test
  def imputesMissingToFirstMode {
    assertArrayEquals(Array(1.toByte, 1.toByte, 1.toByte, 0.toByte, 1.toByte, 1.toByte, 2.toByte,
        2.toByte),
      ModeImputationStrategy(3).impute(Array(BYTE_NA_VALUE, BYTE_NA_VALUE, BYTE_NA_VALUE, 0.toByte, 1.toByte, 1.toByte, 2.toByte, 2.toByte)))
  }
}
