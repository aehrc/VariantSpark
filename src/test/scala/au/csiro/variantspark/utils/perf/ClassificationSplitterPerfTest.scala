package au.csiro.variantspark.utils.perf

import org.junit.Assert._
import org.junit.Test;
import au.csiro.variantspark.algo.ClassificationSplitter
import it.unimi.dsi.util.XorShift1024StarRandomGenerator
import org.apache.spark.mllib.linalg.Vectors
import au.csiro.pbdava.ssparkle.common.utils.Timed
import au.csiro.variantspark.algo.JClassificationSplitter
import scala.collection.immutable.BitSet
import au.csiro.variantspark.algo.FastClassSplitter
import au.csiro.variantspark.algo.ClassSplitter

class ClassificationSplitterPerfTest {
  @Test
  def testSingleSplitter() {
    
    val rg = new XorShift1024StarRandomGenerator(13)
    val nLabels = 10000
    val labels = Array.fill(nLabels)(Math.abs(rg.nextInt) % 2)
    val sp = ClassSplitter(labels, 2)
    val splitIndexes = Range(0, 1000).toArray
    val data = Vectors.dense(Array.fill(nLabels)((Math.abs(rg.nextInt()) % 3).toDouble)).toArray
    Timed.time {
      for (i <- 0 until 50000) {     
        sp.findSplit(data, splitIndexes)
      }
    }.report("Splitting")
    Timed.time {
      for (i <- 0 until 50000) {     
        sp.findSplit(data, splitIndexes)
      }
    }.report("Splitting2")
  }
  
  @Test
  def testJSingleSplitter() {
    
    val rg = new XorShift1024StarRandomGenerator(13)
    val nLabels = 10000
    val labels = Array.fill(nLabels)(Math.abs(rg.nextInt) % 2)
    val sp = new JClassificationSplitter(labels,2)
    val splitIndexes = Range(0, 10000).toArray
    val data = Array.fill(nLabels)((Math.abs(rg.nextInt()) % 3).toDouble)
    Timed.time {
      for (i <- 0 until 50000) {     
        sp.findSplit(data, splitIndexes)
      }
    }.report("Splitting")
    Timed.time {
      for (i <- 0 until 50000) {     
        sp.findSplit(data, splitIndexes)
      }
    }.report("Splitting1")
        Timed.time {
    for (i <- 0 until 50000) {     
        sp.findSplit(data, splitIndexes)
      }
    }.report("Splitting2")
  }
  
  @Test
  def testFastSingleSplitter() {
    
    val rg = new XorShift1024StarRandomGenerator(13)
    val nLabels = 10000
    val labels = Array.fill(nLabels)(Math.abs(rg.nextInt) % 2)
    val sp = new FastClassSplitter()
    val splitIndexes = Range(0, nLabels).toArray
    val data = Array.fill(nLabels)((Math.abs(rg.nextInt()) % 3))
    Timed.time {
      for (i <- 0 until 50000) {     
        sp.findSplit(data, splitIndexes, 2, labels)
      }
    }.report("Splitting")
    Timed.time {
      for (i <- 0 until 50000) {     
        sp.findSplit(data, splitIndexes, 2, labels)
      }
    }.report("Splitting1")
        Timed.time {
    for (i <- 0 until 50000) {     
        sp.findSplit(data, splitIndexes, 2, labels)
      }
    }.report("Splitting2")
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
  
  def findBitmapSplit(data:Array[BitSet], labels:Array[BitSet], split:BitSet) = {
    // assume enrtire set
    
    val totalCount= labels.map(l => ( l & split ).size).toArray
    for (i <- 0 until data.length -1) {
      val leftCount = labels.map(l => ( l & split & data(i)).size).toArray
    }
  }
  
  @Test
  def testBits {
    val rg = new XorShift1024StarRandomGenerator(13)
    val nLabels = 10000
    val labels = Array.fill(nLabels)(Math.abs(rg.nextInt) % 2)
    val splitIndexes = Range(0, 10).toArray
    val data = Array.fill(nLabels)((Math.abs(rg.nextInt()) % 3).toByte)   
    // encode labels as bytes
    val bSplit = BitSet(splitIndexes:_*)
    val bLables = Range(0,2).map(i => BitSet(labels.indices.filter(labels(_) == i).toArray:_*)).toArray
    val bData = Range(0,3).map(i => BitSet(data.indices.filter(labels(_) == i).toArray:_*)).toArray
    Timed.time {
      for (i <- 0 until 50000) {     
        findBitmapSplit(bData, bLables, bSplit)
      }
    }.report("Splitting")
    Timed.time {
      for (i <- 0 until 50000) {     
        findBitmapSplit(bData, bLables, bSplit)
      }
    }.report("Splitting1")
    Timed.time {
      for (i <- 0 until 50000) {     
        findBitmapSplit(bData, bLables, bSplit)
      }
    }.report("Splitting2")  
  }
  

}