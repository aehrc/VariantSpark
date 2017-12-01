package au.csiro.variantspark.api

import org.junit.Test
import org.junit.Assert._
import au.csiro.variantspark.api._
import au.csiro.variantspark.algo.metrics.ManhattanPairwiseMetric



class CommonPairwiseOperationTest {

  @Test
  def canResolveCommonNames() {
    assertEquals(ManhattanPairwiseMetric,CommonPairwiseOperation.withName("manhattan"))
  }
  
}