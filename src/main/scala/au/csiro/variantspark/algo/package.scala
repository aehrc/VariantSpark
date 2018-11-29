package au.csiro.variantspark

import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import au.csiro.variantspark.utils.CanSize
import au.csiro.variantspark.data.Feature

package object algo {
  type IndexedFeature = (Feature,Long)
   
  implicit case object CanSizeFeature extends CanSize[Feature] {
     def size(f:Feature):Int = f.size
  }
  
  implicit case object CanSplitVector extends CanSplit[Vector] {
    override def size(v:Vector) =  v.size
    override def split(v:Vector, splitter: ClassificationSplitter, indices:Array[Int]):SplitInfo = splitter.findSplit(v.toArray, indices)
    override def at(v:Vector)(i:Int):Double =  v(i).toDouble
    def runtimeClass: Class[_] = classOf[Vector]
  }

  implicit case object CanSplitArrayByte extends CanSplit[Array[Byte]] {
    override def size(v:Array[Byte]) =  v.size
    override def split(v:Array[Byte], splitter: ClassificationSplitter, indices:Array[Int]):SplitInfo = splitter.findSplit(v, indices)
    override def at(v:Array[Byte])(i:Int):Double =  v(i).toDouble  
    def runtimeClass: Class[_] = classOf[Array[Byte]]
  }
  
  implicit case object CanSplitSplittable extends CanSplit[Splittable] {
    def at(v: Splittable)(i: Int): Double = v.at(i)
    def size(v: Splittable): Int = v.size  
    def split(v: Splittable, splitter: ClassificationSplitter, indices: Array[Int]): SplitInfo = v.split(splitter, indices)
    def runtimeClass: Class[_] = classOf[Splittable]
  }
    
  implicit def toTreeFeatueRDD(rdd:RDD[TreeFeature]) = new TreeFeatueRDDFunction(rdd)
}