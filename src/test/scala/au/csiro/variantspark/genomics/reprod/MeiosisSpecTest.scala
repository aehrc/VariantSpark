package au.csiro.variantspark.genomics.reprod

import org.junit.Assert._
import org.junit.Test
import au.csiro.variantspark.genomics._

class MeiosisSpecTest {
  @Test
  def testEmptySpecReturnsStartWithChromsome() {
    assertEquals(0, MeiosisSpec(Nil, 0).getChromosomeAt(13L))
    assertEquals(1, MeiosisSpec(Nil, 1).getChromosomeAt(13L))
  }

  @Test
  def testWorksForASingleCrossingOver() {
    val spec0 =  MeiosisSpec(List(13L), 0)
    assertEquals(0, spec0.getChromosomeAt(0L)) 
    assertEquals(0, spec0.getChromosomeAt(12L))     
    assertEquals(1, spec0.getChromosomeAt(13L))     
    val spec1 =  MeiosisSpec(List(13L), 1)
    assertEquals(1, spec1.getChromosomeAt(0L)) 
    assertEquals(1, spec1.getChromosomeAt(12L))     
    assertEquals(0, spec1.getChromosomeAt(13L))     
  }

  @Test
  def testWorksForTwoCrossingOvers() {
    val spec0 =  MeiosisSpec(List(13L, 15L), 0)
    assertEquals(0, spec0.getChromosomeAt(0L)) 
    assertEquals(0, spec0.getChromosomeAt(12L))     
    assertEquals(1, spec0.getChromosomeAt(13L))     
    assertEquals(1, spec0.getChromosomeAt(14L))
    assertEquals(0, spec0.getChromosomeAt(15L))
  }
}



