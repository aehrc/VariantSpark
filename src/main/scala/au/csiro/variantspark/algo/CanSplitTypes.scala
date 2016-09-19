package au.csiro.variantspark.algo

import org.apache.spark.mllib.linalg.Vector
import scala.reflect.ClassTag


class CanSplitVector extends CanSplit[Vector] with Serializable {
  override def size(v:Vector) =  v.size
  override def toArray(v:Vector):Array[Double] = v.toArray
  override def at(i:Int, v:Vector):Int =  v(i).toInt 
}