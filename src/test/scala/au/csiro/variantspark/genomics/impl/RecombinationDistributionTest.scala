package au.csiro.variantspark.genomics.impl

import org.junit.Assert._
import org.junit.Test
import org.easymock.EasyMockSupport
import org.easymock.EasyMock
import org.apache.commons.math3.random.RandomGenerator
import au.csiro.variantspark.genomics.reprod.ContigRecombinationMap
import au.csiro.variantspark.genomics.reprod.RecombinationMap
import au.csiro.variantspark.genomics.reprod.MeiosisSpec


class RecombinationDistributionTest {
  
  val easymock = new EasyMockSupport()
  
  @Test
  def testDrawsNoContigSplitsWhenNotNeed() {
    val rng = easymock.createMock(classOf[RandomGenerator])
    EasyMock.expect(rng.nextDouble()).andReturn(0.51)
    easymock.replayAll();
    val testDistribution = ContigRecombinationDistribution(Array(0L, 2L), Array(0.5))
    assertEquals(List(), testDistribution.drawSplits(rng))        
    easymock.verifyAll();
  }

  @Test
  def testDrawsContigSplitWhenNeed() {
    val rng = easymock.createMock(classOf[RandomGenerator])
    EasyMock.expect(rng.nextDouble()).andReturn(0.45)
    easymock.replayAll();
    val testDistribution = ContigRecombinationDistribution(Array(0L, 2L), Array(0.5))
    assertEquals(List(1L), testDistribution.drawSplits(rng))        
    easymock.verifyAll();
  }

  @Test
  def testCreatesCorrectMeiosisSpec() {
    val rng = easymock.createMock(classOf[RandomGenerator])
    EasyMock.expect(rng.nextDouble()).andReturn(0.5).times(3)
    EasyMock.expect(rng.nextInt(2)).andReturn(1)
    easymock.replayAll();
    val testDistribution = ContigRecombinationDistribution(Array(0L, 2L, 4L, 6L), Array(0.45, 0.55, 0.55))
    assertEquals(MeiosisSpec(List(3L,5l), 1), testDistribution.crossingOver(rng))        
    easymock.verifyAll();
  }

  
  @Test
  def testConstructCorrectlyFromContigRecombinationMap {
    val testMap = ContigRecombinationMap(Array(0L, 1000000L, 3000000L), Array(0.0, 1.0))
    val testDistribution = ContigRecombinationDistribution.fromRecombiationMap(testMap)
    assertArrayEquals(testMap.bins, testDistribution.bins)
    assertArrayEquals(Array(0.0, 2e-2), testDistribution.p, 1e-5)
  }
  
  @Test 
  def testConstructCorrectlyFromRecombinationMap {
    val testRecombinationMap = RecombinationMap(Map(
          "1" -> ContigRecombinationMap(Array(0, 1000L), Array(0.0)),
          "2" -> ContigRecombinationMap(Array(0, 1000L, 2000L), Array(0.0, 1.0))
        ))
    val distribution = RecombinationDistribution.fromRecombiationMap(testRecombinationMap)
    testRecombinationMap.contigMap.foreach { case (k, v) =>
      assertArrayEquals(v.bins, distribution.contigMap(k).bins)
    }
  }
  
  @Test 
  def testCrossingOversOnAllContis {
    val rng = easymock.createMock(classOf[RandomGenerator])
    EasyMock.expect(rng.nextDouble()).andReturn(0.5).times(3)
    EasyMock.expect(rng.nextInt(2)).andReturn(1).times(2)
    easymock.replayAll();
    val tesDistr = RecombinationDistribution(Map(
          "1" -> ContigRecombinationDistribution(Array(0, 1000L), Array(0.0)),
          "2" -> ContigRecombinationDistribution(Array(0, 1000L, 2000L), Array(0.0, 1.0))
        ))
    val result = tesDistr.crossingOver(rng) 
    assertEquals(Map(
        "1" -> MeiosisSpec(List(), 1),
        "2" -> MeiosisSpec(List(1500L), 1)
        ), result)
    easymock.verifyAll();
  }  
}