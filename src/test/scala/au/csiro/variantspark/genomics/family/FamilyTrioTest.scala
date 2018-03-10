package au.csiro.variantspark.genomics.family

import org.junit.Assert._
import org.junit.Test
import java.io.StringReader

class FamilyTrioTest {
  
  val founderTrio = FamilyTrio("FOUNDER", Gender.Male)
  val halfOffspringTrio = FamilyTrio("HALDFOUNDER", Gender.Female, Some("FOUNDER"))
  val offspringTrio = FamilyTrio("OFFSPRING", Gender.Male, Some("FOUNDER"), Some("FOUNDER1"))
  
  val testPedFile="Family ID\tIndivdual ID\tParental ID\tMathernal ID\tGender\tPhenotype\n" + 
    "FAM\tFOUNDER\t0\t0\t1\t0\n" + 
    "FAM\tHALDFOUNDER\tFOUNDER\t0\t2\t0\n" +
    "FAM\tOFFSPRING\tFOUNDER\tFOUNDER1\t1\t0\tXXX\n"
  
  @Test
  def testIsFounder() {
    assertTrue(founderTrio.isFounder);    
    assertFalse(halfOffspringTrio.isFounder);
    assertFalse(offspringTrio.isFounder);    
  }

  @Test
  def testIsFullOffspring() {
    assertFalse(founderTrio.isFullOffspring);    
    assertFalse(halfOffspringTrio.isFullOffspring);
    assertTrue(offspringTrio.isFullOffspring);    
  }

  @Test
  def testGetParents() {
    assertEquals(List.empty, founderTrio.parents);    
    assertEquals(List(("FOUNDER",ParentalRole.Father)), halfOffspringTrio.parents);
    assertEquals(List(("FOUNDER", ParentalRole.Father), ("FOUNDER1", ParentalRole.Mother)),offspringTrio.parents);    
  }
  
  @Test
  def testReadingFromPedLine() {
    assertEquals(founderTrio, FamilyTrio.fromPedLine("FAM FOUNDER 0 0 1 0".split(" ")))
    assertEquals(halfOffspringTrio, FamilyTrio.fromPedLine("FAM HALDFOUNDER FOUNDER 0 2 0".split(" ")))
    assertEquals(offspringTrio, FamilyTrio.fromPedLine("FAM OFFSPRING FOUNDER FOUNDER1 1 0 XXX".split(" ")))
  }
  
  @Test
  def testReadFromPedFile() {
    assertEquals(List(founderTrio,halfOffspringTrio,offspringTrio), 
        FamilyTrio.loadPed(new StringReader(testPedFile))) 
  }
}