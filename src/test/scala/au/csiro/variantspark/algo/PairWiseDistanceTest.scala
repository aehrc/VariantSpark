package au.csiro.variantspark.algo


import au.csiro.variantspark.test.SparkTest
import breeze.linalg.DenseMatrix
import org.junit.Assert._
import org.junit.Test
import au.csiro.variantspark.algo.metrics.EuclideanPairwiseMetric

class PairWiseDistanceTest extends SparkTest {


  @Test
  def testCorrectlyCreateLowerTriang2D() {
    val lowerTriang2D = Array(0.0, 0.3, 0.0)
    val result = PairwiseOperation.lowerTriangWithDiagToMatrix(lowerTriang2D)
    assertEquals(DenseMatrix((0.0, 0.3), (0.3, 0.0)), result)
  }

  @Test
  def testCorrectlyCreateLowerTriang4D() {
    val lowerTriang4D = Array[Double](0, 1, 0 , 2, 3, 0,  4, 5, 6, 0)
    val result = PairwiseOperation.lowerTriangWithDiagToMatrix(lowerTriang4D)
    assertEquals(new DenseMatrix(4, 4, Array[Double](
        0, 1, 2, 4,
        1, 0, 3, 5,
        2, 3, 0, 6, 
        4, 5, 6, 0)), result)
  }

  @Test
  def testCorrectlyCalculatesPairWiseDistance2D() {
    val input = sc.parallelize(List(Array[Byte](0, 1), Array[Byte](0, 2), Array[Byte](1, 1)))
    val result = EuclideanPairwiseMetric.compute(input).value
    assertEquals(3, result.length)
    assertEquals(Math.sqrt(5), result(1), 1e-10)
  }


  @Test
  def testCorrectlyCalculatesPairWiseDistance3d() {
    val input = sc.parallelize(List(Array[Byte](0, 1, 1), Array[Byte](0, 2, 0), Array[Byte](0, 1, 0), Array[Byte](0, 2, 1)), 2)
    val result = EuclideanPairwiseMetric.compute(input).value
    assertEquals(6, result.length)
    assertArrayEquals(Array(0.0,  Math.sqrt(10), 0.0, Math.sqrt(2), Math.sqrt(6), 0.0), result, 1e-10)
  }


}