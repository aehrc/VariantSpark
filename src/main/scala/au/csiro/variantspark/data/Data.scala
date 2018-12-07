package au.csiro.variantspark.data

import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD

trait Data {
  def value: Any
  def valueAsByteArray: Array[Byte]
  def valueAsVector: Vector
  def valueAsStrings: List[String]
  def size:Int
  def at(i:Int):Double  
}


trait DataBuilder[V] {
  def from(l:List[String]): Data
  def from(v:V): Data
  def defaultVariableType: VariableType
}
