package au.csiro.variantspark.pedigree

import org.junit.Assert._
import org.junit.Test

class ContigSetTest {
  
  @Test
  def testParseVcfHeaderLine() {
    val line = "##contig=<ID=1,length=249250621,assembly=b37>"
    val contigSpec = ContigSpec.parseVcfHeaderLine(line)
    assertEquals(ContigSpec("1", 249250621L, Some("b37")), contigSpec)
  }
  
  @Test 
  def testReference_b37_loaded() {
    assertEquals(24,  ReferenceContigSet.b37.contigs.length)
  }
}