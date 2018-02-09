package au.csiro.variantspark.pedigree.impl


import org.junit.Assert._
import org.junit.Test
import it.unimi.dsi.util.XorShift1024StarRandomGenerator

class RecombinationMapTest {
  
  @Test
  def testLoadBedFile() {
    val rm = RecombinationMap.fromBedFile("data/relatedness/genetic_map_GRCh37_1Mb.bed.gz")
  }
}