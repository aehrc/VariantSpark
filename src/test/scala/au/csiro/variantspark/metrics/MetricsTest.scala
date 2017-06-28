package au.csiro.variantspark.metrics

import org.junit.Assert._
import org.junit.Test


class MetricsTest {

  @Test def testRandIndexForPerfectClustering() {
    val clustering = List(0, 0, 0, 0, 1, 1, 1, 1)
    val rIndex = Metrics.adjustedRandIndex(clustering, clustering)
    assertEquals(1.0, rIndex, 1e-15)
  }

  @Test def testRandIndexForWordsClustering() {
    val c1 = List(0, 0, 0, 0, 1, 1, 1, 1)
    val c2 = List(0, 1, 0, 1, 0, 1, 0, 1)
    val rIndex = Metrics.adjustedRandIndex(c1, c2)
    assertEquals(-0.16666666666666674, rIndex, 1e-15)
  }

}
