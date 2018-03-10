package au.csiro.variantspark.genomics.family

import org.junit.Assert._
import org.junit.Test
import au.csiro.variantspark.genomics.reprod.OffspringSpec
import au.csiro.variantspark.genomics.reprod.GameteSpec
import au.csiro.variantspark.genomics._
import org.easymock.EasyMockSupport
import org.easymock.EasyMock
import au.csiro.variantspark.genomics.reprod.MeiosisSpec
import au.csiro.variantspark.genomics.reprod.GameteSpecFactory

class FamilySpecTest  {
  
  val testVariant = TestMutableVariant("22",1L, "A")
  val testGameteSpec: GameteSpec = GameteSpec(Map( "22" -> MeiosisSpec(List.empty, 0)))

  val testPedTree = PedigreeTree(Seq(
        FamilyTrio("F1", Gender.Male, None, None),
        FamilyTrio("F2", Gender.Female, None, None),
        FamilyTrio("F3", Gender.Female, None, None),
        FamilyTrio("OFF1", Gender.Male, Some("F1"),Some("F2")),
        FamilyTrio("OFF2", Gender.Male, Some("OFF1"), Some("F3"))
        ))
          
  val testFamilySpec: FamilySpec = new FamilySpec(Seq(
          Founder("F1", Gender.Male), 
          Founder("F2", Gender.Female), 
          Founder("F3", Gender.Female),
          Offspring("OFF1",  Gender.Male, "F1", "F2", OffspringSpec(testGameteSpec, testGameteSpec)),
          Offspring("OFF2",  Gender.Male, "OFF1", "F3", OffspringSpec(testGameteSpec, testGameteSpec))
  ))
  
  @Test 
  def producesCorrectSummary() {
    assertEquals(FamilySpec.Summary(3,2),  testFamilySpec.summary)
  }
  
  @Test
  def producesCorrectOffspringPool() {
    val initialPool:GenotypePool = Map("F1" -> GenotypeSpec(0,1), "F2" -> GenotypeSpec(2,3), "F3"-> GenotypeSpec(4,5))
    val outputPool = testFamilySpec.produceGenotypePool(testVariant, initialPool)
    assertEquals(Map("F1" -> GenotypeSpec(0,1), "F2" -> GenotypeSpec(2,3), "F3"-> GenotypeSpec(4,5),
        "OFF1"->  GenotypeSpec(0,2), "OFF2"->  GenotypeSpec(0,4)), outputPool)
  }
  
  @Test
  def testBuildFromPedigree() {
    val easymock = new EasyMockSupport()
    val specFactory = easymock.createMock(classOf[GameteSpecFactory])
    EasyMock.expect(specFactory.createGameteSpec()).andReturn(testGameteSpec).times(4)
    easymock.replayAll()
    val familySpec = FamilySpec.apply(testPedTree, specFactory)
    easymock.verifyAll()
    assertEquals(testFamilySpec, familySpec)
  }
}

