package au.csiro.variantspark.utils


import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.commons.math3.random.RandomDataGenerator
import org.apache.spark.rdd.RDD


class Projector(indexSet: Set[Int], include: Boolean = true) extends Serializable {

  def projectVector(v: Vector): Vector = {
    val a = v.toArray
    Vectors.dense((for (i <- a.indices if indexSet.contains(i) == include) yield a(i)).toArray)
  }

  def projectArray(a: Array[Int]): Array[Int] = {
    (for (i <- a.indices if indexSet.contains(i) == include) yield a(i)).toArray
  }
  
  def inverted = new Projector(indexSet, !include)
  
  def toPair = (this, this.inverted)
  
  def indexes = indexSet
}


object Sampling {

    /**
     * Get indexes that should be included in sample from array of this size
     */
    def subsample(size:Int, sampleSize:Int):Array[Int] =  {
      if (sampleSize>size) throw new RuntimeException("Sample size greater then sample len")
      val rdg = new RandomDataGenerator()
      return rdg.nextPermutation(size, sampleSize)
    }
  
    /**
     * Get indexes that should be included in sample from array of this size
     */
    def subsample(size:Int, fraction:Double):Array[Int] =  subsample(size, math.round(size*fraction).toInt)
 
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
  
object Projector {
  
  def apply(indexes:Array[Int], include:Boolean = true) = new Projector(indexes.toSet, include)
  
  def subsample(v:Vector, fraction:Double):Projector = Projector(Sampling.subsample(v.size, fraction))

  def split(v:Vector, fraction:Double):(Projector, Projector) = subsample(v, fraction).toPair
  
  def folds(v:Vector, nFolds:Int, testFolds:Boolean = true):List[Projector] = Sampling.folds(v.size, nFolds).map(Projector(_,testFolds))
  
  def splitRDD(rdd: RDD[Vector], fraction:Double):(Projector, Projector) = split(rdd.first, fraction)
  def rddFolds(rdd:RDD[Vector], nFolds:Int, testFolds:Boolean = true):List[Projector] = folds(rdd.first, nFolds, testFolds)
  
  // TODO: (Refactoring) Find a better place for these (if needed at all
  
  def projectVector(indexSet: Set[Int], invert: Boolean = false)(v: Vector): Vector = {
    val a = v.toArray
    Vectors.dense((for (i <- a.indices if indexSet.contains(i) == !invert) yield a(i)).toArray)
  }

  def projectArray(indexSet: Set[Int], invert: Boolean = false)(a: Array[Int]): Array[Int] = {
    (for (i <- a.indices if indexSet.contains(i) == !invert) yield a(i)).toArray
  }
  
}