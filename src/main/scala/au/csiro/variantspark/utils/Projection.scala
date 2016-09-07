package au.csiro.variantspark.utils

import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.commons.math3.random.RandomDataGenerator
import org.apache.spark.rdd.RDD
import au.csiro.variantspark.utils.defRng

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

object Projector {
  
  def apply(indexes:Array[Int], include:Boolean = true) = new Projector(indexes.toSet, include)
  
  def subsample(v:Vector, fraction:Double):Projector = Projector(Sampling.subsampleFraction(v.size, fraction)(defRng))

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

object RDDProjections {
  implicit def toVectorRDD(rdd:RDD[Vector]) = new VectorRDDFunction(rdd)
  implicit def toIndexedVectorRDD(rdd:RDD[(Vector, Long)]) = new IndexedVectorRDDFunction(rdd)
}

