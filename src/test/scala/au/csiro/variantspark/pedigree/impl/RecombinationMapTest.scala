package au.csiro.variantspark.pedigree.impl


import org.junit.Assert._
import org.junit.Test
import it.unimi.dsi.util.XorShift1024StarRandomGenerator

import org.json4s._
import org.json4s.native.Serialization
import org.json4s.native.Serialization.{read, write}

class RecombinationMapTest {
  
  @Test
  def testLoadBedFile() {
    val rm = RecombinationMap.fromBedFile("tmp/HapMap/hg19/genetic_map_GRCh37.bed.gz")
    println(rm)
    val rng = new XorShift1024StarRandomGenerator(13L)
   
    implicit val formats = Serialization.formats(NoTypeHints)

    for (i <- 0 until 10) {    
      val gameteSpec = HapMapGameteSpecFactory(rm).createHomozigoteSpec()
      val ser = write(gameteSpec)
      println(ser)
    }
  }
}