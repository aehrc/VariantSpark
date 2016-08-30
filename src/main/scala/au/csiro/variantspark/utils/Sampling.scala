package au.csiro.variantspark.utils


import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.commons.math3.random.RandomDataGenerator
import org.apache.spark.rdd.RDD
import org.apache.commons.math3.random.RandomGeneratorFactory
import org.apache.commons.math3.random.RandomGenerator
import org.uncommons.maths.random.XORShiftRNG
import it.unimi.dsi.util.XorShift1024StarRandomGenerator


class Sample(val nSize:Int, val indexes:Array[Int]) {
  def asWeights:Array[Int] = Array()
  def indexesIn:Set[Int] = indexes.toSet
  def indexesOut:Set[Int] = Range(0, nSize).toSet.diff(indexesIn)
  def lenght = indexes.length
}


object Sample {
  def all(nSize:Int) = new Sample(nSize, Range(0, nSize).toArray)
  def fraction(nSize:Int, fraction:Double, withReplacement:Boolean = false) = new Sample(nSize, 
      Sampling.subsampleFraction(nSize, fraction, withReplacement))
}

object Sampling {
  
    /**
     * Get indexes that should be included in sample from array of this size
     */
    def subsample(size:Int, sampleSize:Int, withReplacement:Boolean)(implicit rg:RandomGenerator = new XorShift1024StarRandomGenerator()):Array[Int] =  {
      if (!withReplacement && sampleSize>size) throw new RuntimeException("Sample size greater then sample len")
      val rdg = new RandomDataGenerator(rg)
      return if (withReplacement) 
        Array.fill[Int](sampleSize)(rdg.nextInt(0,size-1)) else rdg.nextPermutation(size, sampleSize)
    }
    
    def subsample(size:Int, sampleSize:Int):Array[Int] = subsample(size, sampleSize, false)
  
    /**
     * Get indexes that should be included in sample from array of this size
     */
    def subsampleFraction(size:Int, fraction:Double, withReplacement:Boolean = false):Array[Int] = 
        subsample(size, if (fraction ==1.0) size else  math.round(size*fraction).toInt, withReplacement)
 
    def folds(size:Int, nFolds:Int):List[Array[Int]] = {
      val rdg = new RandomDataGenerator()
      //check it there is enough to have at least one element per folr
      if (size < nFolds)
        throw new RuntimeException("Not enougth elements")
      val permutation = rdg.nextPermutation(size, size)
      // now I just need to distribute this collections evenly into n folds
      val byFold = Array.range(0, size).groupBy(permutation(_)%nFolds)
      // we do not really care which order we get the folds in
      byFold.values.toList
  }
    
}
