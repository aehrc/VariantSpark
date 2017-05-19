package au.csiro.variantspark.metrics

import org.junit.Assert._
import org.junit.Test;


class GiniTest {

  @Test def testWhenEmptyArrayThenZero() {
    val (gini, total) = Gini.giniImpurityWithTotal(Array())
    assertEquals(0.0, gini, 0.0)
    assertEquals(0, total)
  }

  @Test def testWhenOneClassThenZero() {
    val (gini, total) = Gini.giniImpurityWithTotal(Array(3))
    assertEquals(0.0, gini, 0.0)
    assertEquals(3, total)
  }

  @Test def testWhenPureThenZero() {
    val (gini, total) = Gini.giniImpurityWithTotal(Array(3, 0, 0))
    assertEquals(0.0, gini, 0.0)
    assertEquals(3, total)
  }

  @Test def testWhenEqualClassesThenMax() {
    val (gini, total) = Gini.giniImpurityWithTotal(Array(3, 3, 0))
    assertEquals(0.5, gini, 0.0)
    assertEquals(6, total)
  }

  @Test def testWhenMixedClassesTheCorrectValue() {
    val (gini, total) = Gini.giniImpurityWithTotal(Array(3, 2, 1))
    assertEquals(0.611, gini, 1e-3)
    assertEquals(6, total)
  }
}
