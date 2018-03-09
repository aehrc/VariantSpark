package au.csiro.variantspark.genomics

import org.junit.Assert._
import org.junit.Test

class ContigSpecTest {
  
  @Test 
  def testContigClassifiers() {
    assertTrue(ContigSpec("1",0L).isAutosome)
    assertFalse(ContigSpec("X",0L).isAutosome)
    assertTrue(ContigSpec("X",0L).isSex)
    assertTrue(ContigSpec("Y",0L).isSex)
    assertFalse(ContigSpec("22",0L).isSex)
    assertTrue(ContigSpec("22",0L).isChromosome)
    assertTrue(ContigSpec("X",0L).isChromosome)
    assertTrue(ContigSpec("Y",0L).isChromosome)
    assertFalse(ContigSpec("M00323",0L).isChromosome)   
  }
    
  @Test
  def testParseVcfHeaderLine() {
    val line = "##contig=<ID=1,length=249250621,assembly=b37>"
    val contigSpec = ContigSpec.parseVcfHeaderLine(line)
    assertEquals(ContigSpec("1", 249250621L), contigSpec)
  }  
}

