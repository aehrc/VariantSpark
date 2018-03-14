package au.csiro.variantspark.genomics.impl

import org.junit.Assert._
import org.junit.Test
import org.easymock.EasyMockSupport
import org.easymock.EasyMock
import org.apache.commons.math3.random.RandomGenerator
import au.csiro.variantspark.genomics.reprod.MeiosisSpec
import au.csiro.variantspark.genomics.reprod.RecombinationMap
import au.csiro.variantspark.genomics.reprod.ContigRecombinationMap

class HapMapMeiosisSpecFactoryTest {

  val easymock = new EasyMockSupport()
  
  @Test 
  def testCrossingOversOnAllContis {
    implicit val rng = easymock.createMock(classOf[RandomGenerator])
    EasyMock.expect(rng.nextDouble()).andReturn(0.5).times(3)
    EasyMock.expect(rng.nextInt(2)).andReturn(1).times(2)
    easymock.replayAll();
    val tesDistr = RecombinationDistribution(Map(
          "1" -> ContigRecombinationDistribution(Array(0, 1000L), Array(0.0)),
          "2" -> ContigRecombinationDistribution(Array(0, 1000L, 2000L), Array(0.0, 1.0))
        ))
    val result = HapMapMeiosisSpecFactory(tesDistr).createMeiosisSpec()
    assertEquals(Map(
        "1" -> MeiosisSpec(List(), 1),
        "2" -> MeiosisSpec(List(1500L), 1)
        ), result)
    easymock.verifyAll();
  }  

  @Test 
  def testCrossingOversFromRecombinationMap {
    implicit val rng = easymock.createMock(classOf[RandomGenerator])
    EasyMock.expect(rng.nextDouble()).andReturn(0.5).times(3)
    EasyMock.expect(rng.nextInt(2)).andReturn(1).times(2)
    easymock.replayAll();
    val tesDistr = RecombinationMap(Map(
          "1" -> ContigRecombinationMap(Array(0, 1000L), Array(0.0)),
          "2" -> ContigRecombinationMap(Array(0, 1000000L, 2000000L), Array(0.0, 100.0))
        ))
    val result = HapMapMeiosisSpecFactory(tesDistr).createMeiosisSpec()
    assertEquals(Map(
        "1" -> MeiosisSpec(List(), 1),
        "2" -> MeiosisSpec(List(1500000L), 1)
        ), result)
    easymock.verifyAll();
  }  
  
}