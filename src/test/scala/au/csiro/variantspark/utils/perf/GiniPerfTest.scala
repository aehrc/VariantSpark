package au.csiro.variantspark.utils.perf

import org.junit.Assert._
import org.junit.Test;
import it.unimi.dsi.util.XorShift1024StarRandomGenerator
import au.csiro.pbdava.ssparkle.common.utils.Timed
import au.csiro.variantspark.metrics.Gini
import breeze.linalg.DenseVector
import au.csiro.variantspark.algo.FastGini

class GiniPerfTest {
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
  def testFastGini() {
    val rg = new XorShift1024StarRandomGenerator(13)
    val counters = Array.fill(4)(Math.abs(rg.nextInt))
    Timed.time {
      for (i <- 0 until 100000000) {  
        FastGini.gini(counters)
      }
    }.report("fastGini")
  }
  
  @Test
  def testFastGiniDefault() {
    val rg = new XorShift1024StarRandomGenerator(13)
    val counters = Array.fill(4)(Math.abs(rg.nextInt))
    Timed.time {
      for (i <- 0 until 100000000) {  
        FastGini.defaultGini(counters)
      }
    }.report("fastGiniDefault")
  }
  
}