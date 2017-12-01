package au.csiro.variantspark.api

import org.junit.Test
import org.junit.Assert._
import au.csiro.variantspark.api._
import au.csiro.variantspark.algo.metrics.ManhattanPairwiseMetric
import au.csiro.variantspark.algo.metrics.EuclideanPairwiseMetric
import au.csiro.variantspark.algo.metrics.SharedAltAlleleCount
import au.csiro.variantspark.algo.metrics.AtLeastOneSharedAltAlleleCount



class CommonPairwiseOperationTest {

  @Test
  def canResolveCommonNames() {
    assertEquals(ManhattanPairwiseMetric,CommonPairwiseOperation.withName("manhattan"))
    assertEquals(EuclideanPairwiseMetric,CommonPairwiseOperation.withName("euclidean"))
    assertEquals(SharedAltAlleleCount,CommonPairwiseOperation.withName("sharedAltAlleleCount"))
    assertEquals(AtLeastOneSharedAltAlleleCount,CommonPairwiseOperation.withName("anySharedAltAlleleCount")) 
  }
  
}