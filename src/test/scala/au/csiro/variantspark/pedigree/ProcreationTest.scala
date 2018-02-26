package au.csiro.variantspark.pedigree

import org.junit.Assert._
import org.junit.Test

class GenotypeSpecTest {
  
  @Test
  def testCreateZeroSpec() {        
    val aSpec = GenotypeSpec(0,0)
    assertEquals(0,aSpec._0)
    assertEquals(0,aSpec(0))
    assertEquals(0,aSpec._1)
    assertEquals(0,aSpec(1))
  }  
  
  @Test
  def testCreateValidSpecOdd() {        
    val aSpec = GenotypeSpec(13,19)
    assertEquals(13,aSpec._0)
    assertEquals(13,aSpec(0))
    assertEquals(19,aSpec._1)
    assertEquals(19,aSpec(1))
  }  

  @Test
  def testCreateValidSpec01() {        
    val aSpec = GenotypeSpec(0,1)
    assertEquals(0,aSpec._0)
    assertEquals(0,aSpec(0))
    assertEquals(1,aSpec._1)
    assertEquals(1,aSpec(1))
  }  

  
  @Test
  def testCreateMaxValidSpec() {     
    val maxIndex = 1 << 15 - 1
    val aSpec = GenotypeSpec(maxIndex,maxIndex)
    assertEquals(maxIndex,aSpec._0)
    assertEquals(maxIndex,aSpec(0))
    assertEquals(maxIndex,aSpec._1)
    assertEquals(maxIndex,aSpec(1))
  }
  
  @Test(expected=classOf[AssertionError])
  def testFailOnNegative0() {
     GenotypeSpec.apply(-1,0)
  }

  @Test(expected=classOf[AssertionError])
  def testFailOnNegative1() {
     GenotypeSpec.apply(0,-1)
  }

  @Test(expected=classOf[AssertionError])
  def testFailOnToLarge0() {
     GenotypeSpec.apply(1<<15, 0)
  }

  @Test(expected=classOf[AssertionError])
  def testFailOnToLarge1() {
     GenotypeSpec.apply(0, 1<<15)
  }
}

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

class TestMutableVariant(val contig: ContigID, 
  val pos: Long, 
  val ref: BasesVariant = "C")  extends MutableVariant {
  var altVariant:Option[BasesVariant] = None
  def getOrElseUpdate(alt: BasesVariant): IndexedVariant = {
    altVariant = Some(alt)
    13
  }
}

class GameteSpecTest {
  
  @Test 
  def testExisingContigNoMutation() {
    val spec = GameteSpec(Map("1" -> MeiosisSpec(List(13L)), "2"-> MeiosisSpec(List(13L),1)))
    assertEquals(1, spec.homozigoteAt(new TestMutableVariant("1", 12L), GenotypeSpec(1,2)))
    assertEquals(2, spec.homozigoteAt(new TestMutableVariant("1", 14L), GenotypeSpec(1,2)))
  }
  
  @Test 
  def testExisingContigWithMutationWithMatchingBase {
    val spec = GameteSpec(Map("1" -> MeiosisSpec(List(13L)), "2"-> MeiosisSpec(List(13L),1)), 
        MutationSet(Seq(Mutation(GenomicPos("1", 12L), "C", "G"))))
    val mutationVariant = new TestMutableVariant("1", 12L, "C")
    assertEquals(13, spec.homozigoteAt(mutationVariant, GenotypeSpec(1,2)))
    assertEquals(Some("G"), mutationVariant.altVariant)

  }
  @Test 
  def testExisingContigWithMutationWithMismatchBase {
    val spec = GameteSpec(Map("1" -> MeiosisSpec(List(13L)), "2"-> MeiosisSpec(List(13L),1)), 
        MutationSet(Seq(Mutation(GenomicPos("1", 12L), "C", "G"))))
    val mutationVariant = new TestMutableVariant("1", 12L, "T")
    assertEquals(1, spec.homozigoteAt(mutationVariant, GenotypeSpec(1,2)))
    assertEquals(None, mutationVariant.altVariant)
  }
  
}

