package au.csiro.variantspark.algo


import org.junit.Assert._
import org.junit.Test;
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import au.csiro.variantspark.test.SparkTest
import scala.collection.JavaConversions._
import au.csiro.variantspark.utils.Sample
import scala.collection.mutable.MutableList
import au.csiro.variantspark.data.UnboundedOrdinal
import breeze.linalg.DenseMatrix

class PairWiseDistanceTest extends SparkTest {


  @Test
  def testCorrectlyCreateLowerTriang2D() {
    val lowerTriang2D = Array(0.3)
    val result = PairwiseDistance.lowerTraingToMatrix(lowerTriang2D, 2)
    assertEquals(DenseMatrix((0.0, 0.3), (0.3, 0.0)), result)
  }

  @Test
  def testCorrectlyCreateLowerTriang4D() {
    val lowerTriang4D = Array[Double](1, 2, 3, 4, 5, 6)
    val result = PairwiseDistance.lowerTraingToMatrix(lowerTriang4D, 4)
    assertEquals(new DenseMatrix(4, 4, Array[Double](0, 1, 2, 4, 1, 0, 3, 5, 2, 3, 0, 6, 4, 5, 6, 0)), result)
  }


  @Test
  def testCorrectlCalculatesPairWiseDistance2D() {
    val input = sc.parallelize(List(Array[Byte](0, 1), Array[Byte](0, 2), Array[Byte](1, 1)))
    val result = PairwiseDistance().compute(input)
    assertEquals(1, result.length)
    assertEquals(Math.sqrt(5), result(0), 1e-10)
  }


  @Test
  def testCorrectlCalculatesPairWiseDistance3d() {
    val input = sc.parallelize(List(Array[Byte](0, 1, 1), Array[Byte](0, 2, 0), Array[Byte](0, 1, 0), Array[Byte](0, 2, 1)), 2)
    val result = PairwiseDistance().compute(input)
    assertEquals(3, result.length)
    assertArrayEquals(Array(Math.sqrt(10), Math.sqrt(2), Math.sqrt(6)), result, 1e-10)
  }


}