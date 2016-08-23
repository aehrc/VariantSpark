package au.csiro.variantspark.utils

import org.junit.Assert._
import org.junit.Test;

class SampleTest {
  @Test
  def testFullSampleWithReplacement() {
    val nSize = 100
    val sample = Sample.fraction(nSize, 1.0, true)
    assertEquals(nSize, sample.nSize)
    assertEquals(nSize, sample.indexes.length)
    assertTrue("All indexes positive", sample.indexes.forall(_ >=0))
    assertTrue("All indexes less then nSize", sample.indexes.forall(_ < nSize))
    assertTrue("There are out of bag indexes", !sample.indexesOut.isEmpty)
    assertTrue("There are in  bag indexes", !sample.indexesIn.isEmpty) 
  }

  
  @Test
  def testFullSampleWithoutReplacement() {
    val nSize = 100
    val sample = Sample.fraction(nSize, 1.0, false)
    assertEquals(nSize, sample.nSize)
    assertEquals(nSize, sample.lenght)
    assertTrue("All indexes positive", sample.indexes.forall(_ >=0))
    assertTrue("All indexes less then nSize", sample.indexes.forall(_ < nSize))
    assertTrue("There are no out of bag indexes", sample.indexesOut.isEmpty)
    assertEquals("There are in  bag indexes", nSize, sample.indexesIn.size) 
  }
  
  @Test
  def testFreactionSampleWithoutReplacement() {
    val nSize = 100
    val fraction = 0.5
    val sample = Sample.fraction(nSize, fraction, false)
    assertEquals(nSize, sample.nSize)
    assertEquals(nSize/2, sample.lenght)
    assertTrue("All indexes positive", sample.indexes.forall(_ >=0))
    assertTrue("All indexes less then nSize", sample.indexes.forall(_ < nSize))
    assertEquals(sample.lenght, sample.indexesIn.size)
    assertEquals(sample.lenght, sample.indexesOut.size)
  }
  
  
  @Test
  def testFreactionSampleWithReplacement() {
    val nSize = 100
    val fraction = 0.5
    val sample = Sample.fraction(nSize, fraction, true)
    assertEquals(nSize, sample.nSize)
    assertEquals(nSize/2, sample.lenght)
    assertTrue("All indexes positive", sample.indexes.forall(_ >=0))
    assertTrue("All indexes less then nSize", sample.indexes.forall(_ < nSize))
    assertTrue(sample.lenght > sample.indexesIn.size)
    assertTrue(sample.lenght < sample.indexesOut.size)
  }
}