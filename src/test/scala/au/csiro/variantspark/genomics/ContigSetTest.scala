package au.csiro.variantspark.genomics

import org.junit.Assert._
import org.junit.Test
import java.io.ByteArrayInputStream

class ContigSetTest {

  val testContigSet = ContigSet(Seq(ContigSpec("1",1L), ContigSpec("2",2L), ContigSpec("X",3L), ContigSpec("MC23232",4L)))
  val testVcfHeader =
"""##contig=<ID=1,length=1,assembly=b37>
##contig=<ID=2,length=2,assembly=b37>
##contig=<ID=X,length=3,assembly=b37>
##contig=<ID=MC23232,length=4>
##other header
"""
  
  @Test
  def testCalculatesTotalLenghtCorrectly() {
    assertEquals(10L, testContigSet.totalLenght);
  }
  
  @Test
  def testFiltersAutosomesCorrectly() {
    assertEquals(ContigSet(Seq(ContigSpec("1",1L), ContigSpec("2",2L))),
       testContigSet.onlyAutosomes()) 
  }
  
  @Test
  def testGetsConfigIdsCorrectly() {
    assertEquals(Set("1", "2", "X","MC23232"), testContigSet.contigIds) 
  }
  
  @Test 
  def testLoadsFromVCFHeadr() {
    
    assertEquals(testContigSet, 
        ContigSet.fromVcfHeader(new ByteArrayInputStream(testVcfHeader.getBytes)));
  }
 
  @Test 
  def testCannonicalOrdering() {
    assertEquals(Seq(ContigSpec("1",1L), ContigSpec("2",2L), ContigSpec("MC23232",4L), ContigSpec("X",3L)),
        testContigSet.toCannonicalSeq)
  }
  
  @Test 
  def testReference_b37_loaded() {
    assertEquals(24,  ReferenceContigSet.b37.contigs.size)
  }
}

