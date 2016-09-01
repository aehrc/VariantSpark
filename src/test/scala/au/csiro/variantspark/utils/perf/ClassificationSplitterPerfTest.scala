package au.csiro.variantspark.utils.perf

import org.junit.Assert._
import org.junit.Test;
import au.csiro.variantspark.algo.ClassificationSplitter
import it.unimi.dsi.util.XorShift1024StarRandomGenerator
import org.apache.spark.mllib.linalg.Vectors
import au.csiro.pbdava.ssparkle.common.utils.Timed

class ClassificationSplitterPerfTest {
  @Test
  def testSingleSplitter() {
    
    val rg = new XorShift1024StarRandomGenerator(13)
    val nLabels = 10000
    val labels = Array.fill(nLabels)(Math.abs(rg.nextInt) % 2)
    val sp = ClassificationSplitter(labels, 1.0)
    val splitIndexes = Range(0, nLabels).toArray
    val data = Vectors.dense(Array.fill(nLabels)((Math.abs(rg.nextInt()) % 5).toDouble))
    Timed.time {
      for (i <- 0 until 10000) {     
        sp.findSplit(data, splitIndexes)
      }
    }.report("Splitting")
  }
  
  
  @Test
  def testMultipleSplitter() {
    implicit val rg = new XorShift1024StarRandomGenerator(13)
    val nLabels = 10000
    val labels = Array.fill(nLabels)(Math.abs(rg.nextInt) % 2)
    val sp = ClassificationSplitter(labels, 1.0)
    val splitIndexes = Range(0, nLabels).toArray
    val data = Vectors.dense(Array.fill(nLabels)((Math.abs(rg.nextInt()) % 3).toDouble))
    Timed.time {
      for (i <- 0 until 1000) {     
        sp.findSplits(data, Array.fill(10)(splitIndexes))
      }
    }.report("Splitting")
  }
  
  @Test 
  def testAdditions () {
    var in = 0
    val arr = Array.fill(1)(0)
     Timed.time {
        for (i <- 0 until 1000000) {     
          arr(0) += 1
        }
      }.report("Array")   
    Timed.time {
        for (i <- 0 until 1000000) {     
          in += 1
        }
      }.report("Variable")  
   }
  

}