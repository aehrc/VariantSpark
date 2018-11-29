package au.csiro.variantspark.data

import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD

trait Feature {
  def variableType: VariableType
  def label:String
  def value: Any
  def valueAsByteArray: Array[Byte]
  def valueAsVector: Vector
  def valueAsStrings: List[String]
  def size:Int
  def at(i:Int):Double
}

trait FeatureBuilder[V] {
  def from(label: String, variableType:VariableType, l:List[String]): Feature
  def from(label: String, variableType:VariableType, v:V): Feature
  //def from(label: String, variableType:VariableType, l:Array[Byte]):Feature
}


class ToFeature[V](val v:RDD[V]) extends AnyVal {
  def asFeature(variableType:VariableType)(implicit fb:FeatureBuilder[V]):RDD[(Feature,Long)] = {
    v.map(v => fb.from(null, variableType, v)).zipWithIndex
  }
}