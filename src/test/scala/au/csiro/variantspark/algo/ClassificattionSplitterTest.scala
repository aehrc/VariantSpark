package au.csiro.variantspark.algo


import org.junit.Assert._
import org.junit.Test;
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.Logging

abstract class ClassificattionSplitterTest extends Logging {
    
  def splitter(labels:Array[Int], nLabels:Int = 2):ClassificationSplitter
 
  @Test
  def testEmptySplit() {
    val splitInfo = splitter(Array(1)).findSplit(Vectors.dense(0.0).toArray, Array[Int]())
    assertNull(splitInfo)
  }
  
  @Test
  def testOneElementSplit() {
    val splitInfo = splitter(Array(1)).findSplit(Vectors.dense(0.0).toArray, Array(0))
    assertNull(splitInfo)
  }

  
  @Test
  def testConstansLabelSplit() {
    val splitInfo = splitter(Array(1, 1, 1, 1)).findSplit(Vectors.dense(0.0, 1.0, 2.0, 3.0).toArray, Range(0,4).toArray)
    assertEquals(SplitInfo(0,0.0, 0.0, 0.0), splitInfo)
  }

  @Test
  def testConstantsValuesSplist() {
    val splitInfo = splitter(Array(0, 1, 0, 1)).findSplit(Vectors.dense(1.0, 1.0, 1.0, 1.0).toArray, Range(0,4).toArray)
    assertNull(splitInfo)
  }

  
  @Test
  def testActualSplit() {
    val splitInfo = splitter(Array(0, 1, 0, 1)).findSplit(Vectors.dense(0.0, 2.0, 1.0, 2.0).toArray, Range(0,4).toArray)
    assertEquals(SplitInfo(1,0.0, 0.0, 0.0), splitInfo)
  }
  
  
  @Test
  def testActualSplitWithSubset() {
    val splitInfo = splitter(Array(0, 1, 0, 1, 0, 0)).findSplit(Vectors.dense(0.0, 2.0, 1.0, 2.0, 2.0, 2.0).toArray, Range(0,4).toArray)
    assertEquals(SplitInfo(1,0.0, 0.0, 0.0), splitInfo)
  }

  
  @Test
  def testGiniWithComplexSplit() {
    val splitInfo = splitter(Array(0, 1, 0, 0, 1, 1, 0)).findSplit(Vectors.dense(0.0, 0.0, 1.0, 1.0,  2.0, 3.0, 3.0).toArray, Range(0,7).toArray)
    val rightGini = 1.0-(0.25*0.25 + 0.75*0.75)
    val leftGini = 1 - 5.0/9.0 // 1 - (1/3^ + 2/3^2)
    assertEquals(SplitInfo(1, (4.0 * rightGini + 3.0 * leftGini) / 7.0, rightGini, leftGini), splitInfo)
  }
   
}

class JClassificationSplitterTest extends ClassificattionSplitterTest {
  def splitter(labels:Array[Int], nLabels:Int = 2) = new JClassificationSplitter(labels,nLabels, 4)
}


class JClassificationSplitterUnboundedTest extends ClassificattionSplitterTest {
  def splitter(labels:Array[Int], nLabels:Int = 2) = new JClassificationSplitter(labels,nLabels)
}

class JConfusionClassificationSplitterTest extends ClassificattionSplitterTest {
  def splitter(labels:Array[Int], nLabels:Int = 2) = new JConfusionClassificationSplitter(labels,nLabels, 4)
}


