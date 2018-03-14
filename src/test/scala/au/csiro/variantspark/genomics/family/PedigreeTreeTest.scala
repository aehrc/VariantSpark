package au.csiro.variantspark.genomics.family

import org.junit.Assert._
import org.junit.Test

class PedigreeTreeTest {
  
  val testTrios = Seq(
          FamilyTrio("FUN1", Gender.Male), 
          FamilyTrio("FUN2", Gender.Female),
          FamilyTrio("FUN3", Gender.Female),
          FamilyTrio("OFF1", Gender.Male, Some("FUN1"), Some("FUN2")),
          FamilyTrio("OFF2", Gender.Female, Some("OFF1"), Some("FUN3")) 
        )

  val testTree = PedigreeTree(testTrios)  
  
  @Test
  def testLoadFromPedFile() {
    assertEquals(testTree, PedigreeTree.loadPed("src/test/data/simple_ped.ped"))
  }
  
  @Test
  def testTopologicalSort() {    
    val orderedTrios = testTree.orderedTrios
    assertEquals(testTrios.toSet, orderedTrios.toSet)
    val orderedTrioIds = testTree.orderedTrioIds
    assertEquals(orderedTrios.map(_.id), orderedTrioIds)
    assertTrue(orderedTrioIds.indexOf("FUN1") < orderedTrioIds.indexOf("OFF1"))
    assertTrue(orderedTrioIds.indexOf("FUN2") < orderedTrioIds.indexOf("OFF1"))
    assertTrue(orderedTrioIds.indexOf("OFF1") < orderedTrioIds.indexOf("OFF2"))
    assertTrue(orderedTrioIds.indexOf("FUN3") < orderedTrioIds.indexOf("OFF2"))
  }
  
}
