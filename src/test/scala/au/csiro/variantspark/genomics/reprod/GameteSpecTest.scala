package au.csiro.variantspark.genomics.reprod

import org.junit.Assert._
import org.junit.Test
import au.csiro.variantspark.genomics._

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
        MutationSet(Seq(Mutation(GenomicCoord("1", 12L), "C", "G"))))
    val mutationVariant = new TestMutableVariant("1", 12L, "C")
    assertEquals(13, spec.homozigoteAt(mutationVariant, GenotypeSpec(1,2)))
    assertEquals(Some("G"), mutationVariant.altVariant)

  }
  @Test 
  def testExisingContigWithMutationWithMismatchBase {
    val spec = GameteSpec(Map("1" -> MeiosisSpec(List(13L)), "2"-> MeiosisSpec(List(13L),1)), 
        MutationSet(Seq(Mutation(GenomicCoord("1", 12L), "C", "G"))))
    val mutationVariant = new TestMutableVariant("1", 12L, "T")
    assertEquals(1, spec.homozigoteAt(mutationVariant, GenotypeSpec(1,2)))
    assertEquals(None, mutationVariant.altVariant)
  }
  
}

