package au.csiro.variantspark.genomics.reprod

import org.junit.Assert._
import org.junit.Test
import au.csiro.variantspark.genomics._

class GenotypeSpecTest {
  
  @Test
  def testCreateZeroSpec() {        
    val aSpec = GenotypeSpec(0,0)
    assertEquals(0,aSpec._0)
    assertEquals(0,aSpec(0))
    assertEquals(0,aSpec._1)
    assertEquals(0,aSpec(1))
  }  
  
  @Test
  def testCreateValidSpecOdd() {        
    val aSpec = GenotypeSpec(13,19)
    assertEquals(13,aSpec._0)
    assertEquals(13,aSpec(0))
    assertEquals(19,aSpec._1)
    assertEquals(19,aSpec(1))
  }  

  @Test
  def testCreateValidSpec01() {        
    val aSpec = GenotypeSpec(0,1)
    assertEquals(0,aSpec._0)
    assertEquals(0,aSpec(0))
    assertEquals(1,aSpec._1)
    assertEquals(1,aSpec(1))
  }  

  
  @Test
  def testCreateMaxValidSpec() {     
    val maxIndex = 1 << 15 - 1
    val aSpec = GenotypeSpec(maxIndex,maxIndex)
    assertEquals(maxIndex,aSpec._0)
    assertEquals(maxIndex,aSpec(0))
    assertEquals(maxIndex,aSpec._1)
    assertEquals(maxIndex,aSpec(1))
  }
  
  @Test(expected=classOf[AssertionError])
  def testFailOnNegative0() {
     GenotypeSpec.apply(-1,0)
  }

  @Test(expected=classOf[AssertionError])
  def testFailOnNegative1() {
     GenotypeSpec.apply(0,-1)
  }

  @Test(expected=classOf[AssertionError])
  def testFailOnToLarge0() {
     GenotypeSpec.apply(1<<15, 0)
  }

  @Test(expected=classOf[AssertionError])
  def testFailOnToLarge1() {
     GenotypeSpec.apply(0, 1<<15)
  }
}
