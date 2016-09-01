package au.csiro.variantspark.utils.perf

import org.junit.Assert._
import org.junit.Test;
import it.unimi.dsi.util.XorShift1024StarRandomGenerator
import au.csiro.pbdava.ssparkle.common.utils.Timed
import au.csiro.variantspark.metrics.Gini
import au.csiro.variantspark.metrics.GiniBreeze
import breeze.linalg.DenseVector

class GiniTest {
  @Test
  def testGini() {
    val rg = new XorShift1024StarRandomGenerator(13)
    val counters = Array.fill(2)(Math.abs(rg.nextInt))
    Timed.time {
      for (i <- 0 until 1000000) {  
        Gini.giniImpurityWithTotal(counters)
      }
    }.report("gini")
  }
  
  @Test
  def testGiniBreeze() {
    val rg = new XorShift1024StarRandomGenerator(13)
    val countersArray = Array.fill(2)(Math.abs(rg.nextInt))
    val counters = DenseVector(countersArray)
    Timed.time {
      for (i <- 0 until 1000000) {  
        GiniBreeze.giniImpurityWithTotal(counters)
      }
    }.report("giniBreeze")
  }
}