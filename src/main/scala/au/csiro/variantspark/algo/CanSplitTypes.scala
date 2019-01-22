package au.csiro.variantspark.algo

import org.apache.spark.mllib.linalg.Vector
import scala.reflect.ClassTag

@SerialVersionUID(1l)
class CanSplitVector extends CanSplit[Vector] with Serializable {
  override def size(v:Vector) =  v.size
  override def split(v:Vector, splitter: ClassificationSplitter, indices:Array[Int]):SplitInfo = splitter.findSplit(v.toArray, indices)
  override def at(v:Vector)(i:Int):Int =  v(i).toInt 
  def runtimeClass: Class[_] = classOf[Vector]

}

@SerialVersionUID(1l)
class CanSplitArrayByte extends CanSplit[Array[Byte]] with Serializable {
  override def size(v:Array[Byte]) =  v.size
  override def split(v:Array[Byte], splitter: ClassificationSplitter, indices:Array[Int]):SplitInfo = splitter.findSplit(v, indices)
  override def at(v:Array[Byte])(i:Int):Int =  v(i).toInt  
  def runtimeClass: Class[_] = classOf[Array[Byte]]
}