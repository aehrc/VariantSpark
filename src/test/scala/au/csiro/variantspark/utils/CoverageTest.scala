package au.csiro.variantspark.utils

import org.apache.commons.math3.random.JDKRandomGenerator
import org.junit.Assert._
import org.junit.Test
import au.csiro.pbdava.ssparkle.common.utils.Timer


class CodeCoverageTests {
  implicit val rng = new JDKRandomGenerator()

  @Test
  def helloWorldTest() {
    val myTruth = true
    val myNum = 1
    val myHello = "hello variant-spark"
    assertTrue("This should be true", myTruth)
    assertEquals(1,myNum)
    assertEquals("hello variant-spark",myHello)
  }


  @Test
  def testScalaTimerDuration() {
    val myTimer = Timer()
    val myStartTime = System.currentTimeMillis()
    val elapsed = System.currentTimeMillis() - myStartTime
    assertEquals(myTimer.duration,elapsed)
  }
}