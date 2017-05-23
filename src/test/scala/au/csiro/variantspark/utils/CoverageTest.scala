package au.csiro.variantspark.utils

import org.apache.commons.math3.random.JDKRandomGenerator
import org.junit.Assert._
import org.junit.Test
import au.csiro.pbdava.ssparkle.common.utils.Timer


class CoverageTest {
  implicit val rng = new JDKRandomGenerator()

  @Test
  def firstTest() {
    val myTruth = true
    val myNum = 1
    assertTrue("This should be true", myTruth)
    assertEquals(1,myNum)
  }


  @Test
  def testScalaTimerDuration() {
    val myTimer = Timer()
    val duration = 0
    assertEquals(myTimer.duration,duration)
  }
}