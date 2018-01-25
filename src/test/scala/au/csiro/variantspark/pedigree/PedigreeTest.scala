package au.csiro.variantspark.pedigree

import org.junit.Assert._
import org.junit.Test

class PedigreeTest {

  @Test
  def testBuildSamplePedigree()  {
    
    val ped = Offspring("C2", Founder("F3"), Offspring("C1", Founder("F1"), Founder("F2")))
    println(ped)
  
    
    
  }
  
}