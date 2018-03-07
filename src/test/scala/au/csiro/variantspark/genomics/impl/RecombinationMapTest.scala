package au.csiro.variantspark.genomics.impl


import org.junit.Assert._
import org.junit.Test
import it.unimi.dsi.util.XorShift1024StarRandomGenerator


import org.json4s._
import org.json4s.jackson.Serialization

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