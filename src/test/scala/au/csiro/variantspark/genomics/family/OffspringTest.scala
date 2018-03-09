package au.csiro.variantspark.genomics.family

import org.junit.Assert._
import org.junit.Test
import au.csiro.variantspark.genomics.OffspringSpec
import au.csiro.variantspark.genomics.GameteSpec
import au.csiro.variantspark.genomics.Gender
import au.csiro.variantspark.genomics._

class OffspringTest  {
  
  val testVariant = TestMutableVariant("22",1L, "A")
  val testGameteSpec: GameteSpec = GameteSpec(Map( "22" -> MeiosisSpec(List.empty, 0)))

  @Test 
  def testProducesCorrrectGenotypeFromPool() {
    val offspringSpec = OffspringSpec(testGameteSpec, testGameteSpec)
    val testOffspring = Offspring("OFF", Gender.Female, "FAT", "MOT", offspringSpec)
    val pool:GenotypePool = Map("FAT" -> GenotypeSpec(0,1), "MOT" -> GenotypeSpec(2,3))
    val resultGenotype = testOffspring.makeGenotype(testVariant, pool)
    assertEquals(GenotypeSpec(0,2), resultGenotype)
  }
}

