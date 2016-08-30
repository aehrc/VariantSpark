package au.csiro.variantspark.algo


import org.junit.Assert._
import org.junit.Test;
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.Logging

class ClassificattionSplitterTest extends Logging {
    
  @Test
  def testEmptySplit() {
    val splitInfo = ClassificationSplitter(Array(1)).findSplit(Vectors.dense(0.0), Array())
    assertEquals(None, splitInfo)
  }
  
  @Test
  def testOneElementSplit() {
    val splitInfo = ClassificationSplitter(Array(1)).findSplit(Vectors.dense(0.0), Array(0))
    assertEquals(None, splitInfo)
  }

  
  @Test
  def testConstansLabelSplit() {
    val splitInfo = ClassificationSplitter(Array(1, 1, 1, 1)).findSplit(Vectors.dense(0.0, 1.0, 2.0, 3.0), Range(0,4).toArray)
    assertEquals(SplitInfo(0,0.0, 0.0, 0.0), splitInfo.get)
  }

  @Test
  def testConstantsValuesSplist() {
    val splitInfo = ClassificationSplitter(Array(0, 1, 0, 2)).findSplit(Vectors.dense(1.0, 1.0, 1.0, 1.0), Range(0,4).toArray)
    assertEquals(None, splitInfo)
  }

  
  @Test
  def testActualSplit() {
    val splitInfo = ClassificationSplitter(Array(0, 1, 0, 1)).findSplit(Vectors.dense(0.0, 2.0, 1.0, 2.0), Range(0,4).toArray)
    assertEquals(SplitInfo(1,0.0, 0.0, 0.0), splitInfo.get)
  }
  
  
  @Test
  def testActualSplitWithSubset() {
    val splitInfo = ClassificationSplitter(Array(0, 1, 0, 1, 0, 0)).findSplit(Vectors.dense(0.0, 2.0, 1.0, 2.0, 2.0, 2.0), Range(0,4).toArray)
    assertEquals(SplitInfo(1,0.0, 0.0, 0.0), splitInfo.get)
  }

  
  @Test
  def testGiniWithComplexSplit() {
    val splitInfo = ClassificationSplitter(Array(0, 1, 0, 0, 1, 1, 0)).findSplit(Vectors.dense(0.0, 0.0, 1.0, 1.0,  2.0, 3.0, 3.0), Range(0,7).toArray)
    val rightGini = 1.0-(0.25*0.25 + 0.75*0.75)
    val leftGini = 1 - 5.0/9.0 // 1 - (1/3^ + 2/3^2)
    assertEquals(SplitInfo(1, (4.0 * rightGini + 3.0 * leftGini) / 7.0, rightGini, leftGini), splitInfo.get)
  }
   
  
}