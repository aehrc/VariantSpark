package au.csiro.variantspark.data

import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD

trait DataLike {
  def size:Int
  def at(i:Int):Double    
}

trait Data extends DataLike {
  def value: Any
  def valueAsByteArray: Array[Byte]
  def valueAsIntArray: Array[Int]
  def valueAsVector: Vector
  def valueAsStrings: List[String]
}


trait DataBuilder[V] {
  def from(l:List[String]): Data
  def from(v:V): Data
  def defaultVariableType: VariableType
}
