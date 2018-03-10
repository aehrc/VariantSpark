package au.csiro.variantspark.genomics.reprod


import org.junit.Assert._
import org.junit.Test
import org.json4s._
import au.csiro.variantspark.genomics.reprod.RecombinationMap
import au.csiro.variantspark.genomics.ReferenceContigSet

class RecombinationMapTest {
  
  @Test
  def testLoadBedFile() {
    val rm = RecombinationMap.fromBedFile("data/relatedness/genetic_map_GRCh37_1Mb.bed.gz")    
    assertEquals(22, rm.contigMap.size)
    val contigSpec = rm.toContigSpecSet;
    println("Contig Set")
    println(contigSpec)
    println(ReferenceContigSet.b37)
  }
}