package au.csiro.variantspark.genomics.reprod


import org.junit.Assert._
import org.junit.Test
import org.json4s._
import au.csiro.variantspark.genomics.ReferenceContigSet
import scala.io.Source
import au.csiro.variantspark.genomics.ContigSpec
import au.csiro.variantspark.genomics.ContigSet

class RecombinationMapTest {
  
  val testBedContents:String = 
"chr1\t0\t1000\t0.000000\t0.000000\n" + 
"chr1\t1000\t2000\t1.000000\t0.000000\n" + 
"chr2\t0\t500\t2.000000\t0.000000\n"

  val testRecombinationMap = Map(
        "1" -> ContigRecombinationMap(Array(0L, 1000L, 2000L), Array(0.0, 1.0)),
        "2" -> ContigRecombinationMap(Array(0L, 500L), Array(2.0))
      )
   
      
  @Test 
  def testComputeBinSizes() {
    assertArrayEquals(Array(1000L, 1000L), testRecombinationMap("1").binLengths)
    assertArrayEquals(Array(500L), testRecombinationMap("2").binLengths)    
  }
  
  @Test
  def testLoadBedFromSource() {
    val loadedMap = RecombinationMap.fromSource(Source.fromString(testBedContents))
    testRecombinationMap.foreach { case (k, v) =>
      val contigMap = loadedMap.contigMap(k)
      assertArrayEquals(v.bins, contigMap.bins)
      assertArrayEquals(v.recombFreq, contigMap.recombFreq, 1e-5)
   }
  }

  @Test 
  def testToConfigSpec() {
    val contigSpec =  RecombinationMap(testRecombinationMap).toContigSpecSet
    assertEquals(ContigSet(Set(ContigSpec("1", 2000L), ContigSpec("2", 500L))),  contigSpec)
  }
  
  @Test
  def testLoadBedFile() {
    val rm = RecombinationMap.fromBedFile("data/relatedness/genetic_map_GRCh37_1Mb.bed.gz")    
    assertEquals(22, rm.contigMap.size)
    val contigSpec = rm.toContigSpecSet;
    assertEquals(ReferenceContigSet.b37.onlyAutosomes(), contigSpec)
  }
}