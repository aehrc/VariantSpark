package au.csiro.variantspark.algo.impurity


import org.junit.Assert._
import org.junit.Test

class FastGiniTest {

  def checkGini(gini: Double, counts: Array[Int], precision: Double = 0.0) {
    assertEquals(gini, FastGini.gini(counts), precision)
    assertEquals(gini, FastGini.defaultGini(counts), precision)
  }

  @Test def testWhenEmptyArrayThenZero() {
    checkGini(0.0, Array())
  }

  @Test def testWhenOneClassThenZero() {
    checkGini(0.0, Array(3))
  }

  @Test def testWhenPureThenZero() {
    checkGini(0.0, Array(3, 0, 0))
  }

  @Test def testWhenEqualClassesThenMax() {
    checkGini(0.5, Array(3, 3))
  }

  @Test def testWhenMixedClassesTheCorrectValue() {
    checkGini((1.0 - 5.0 / 9.0), Array(1, 2), 1e-4)
    checkGini((1.0 - 14.0 / 36.0), Array(1, 2, 3), 1e-4)
    checkGini((1.0 - 30.0 / 100.0), Array(1, 2, 3, 4), 1e-4)
    checkGini((1.0 - 55.0 / 225.0), Array(1, 2, 3, 4, 5), 1e-4)
  }
}