package au.csiro.variantspark.algo


import org.junit.Assert._
import org.junit.Test

class ArrayOpsTest {
   
  @Test
  def testDenseRankWithOnEmptyArray() {
    
    val values = Array[Double]()
    val rank = Array.ofDim[Int](values.size);
    val rankValues = ArrayOps.denseRank(values, rank)    
    assertArrayEquals(Array.emptyDoubleArray, rankValues, 1e-6)
    assertArrayEquals(Array.emptyIntArray, rank)
  }

  @Test
  def testDenseRankWithOneElement() {
    
    val values = Array[Double](1.0)
    val rank = Array.ofDim[Int](values.size);
    val rankValues = ArrayOps.denseRank(values, rank)    
    assertArrayEquals(Array(1.0), rankValues, 1e-6)
    assertArrayEquals(Array(0), rank)
  }

  
  @Test
  def testDenseRankWithUniqueValues() {
    
    val values = Array(4.0, 1.0, 3.0, 2.0)
    val rank = Array.ofDim[Int](values.size);    
    val rankValues = ArrayOps.denseRank(values, rank)
    assertArrayEquals(Array(1.0, 2.0, 3.0, 4.0), rankValues, 1e-6)
    assertArrayEquals(Array(3, 0, 2, 1), rank)
  }

  
  @Test
  def testDenseRankWithRepeatedValues() {
    
    val values = Array(1.0, 3.0, 2.0, 3.0, 1.0)
    val rank = Array.ofDim[Int](values.size);    
    val rankValues = ArrayOps.denseRank(values, rank)
    assertArrayEquals(Array(1.0, 2.0, 3.0), rankValues, 1e-6)
    assertArrayEquals(Array(0, 2, 1, 2, 0), rank)
  }
}