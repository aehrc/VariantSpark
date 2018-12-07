package au.csiro.variantspark.data

import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD

/**
 * TODO: Rethingk this inheritance
 */
trait Feature extends Data {
  def label:String
  def variableType: VariableType
  def data: Data
}

case class StdFeature(label:String, variableType: VariableType, data:Data) extends Feature {
  def at(i: Int): Double = data.at(i)
  def size: Int = data.size
  def value: Any = data.value
  def valueAsByteArray:Array[Byte] = data.valueAsByteArray
  def valueAsStrings: List[String] = data.valueAsStrings
  def valueAsVector: Vector = data.valueAsVector
}

object StdFeature {
  def from[V](label: String, variableType:VariableType, v:V)(implicit db:DataBuilder[V]): Feature = {
    StdFeature(label, variableType, db.from(v))
  }
  def from[V](label: String, v:V)(implicit db:DataBuilder[V]): Feature = from(label, db.defaultVariableType, v)
  def from[V](label: String, variableType:VariableType, v:List[String])(implicit db:DataBuilder[V]): Feature = StdFeature(label, variableType, db.from(v))
}

trait FeatureBuilder {
  def from(label: String, variableType:VariableType, l:List[String]): Feature
}


class ToFeature[V](val v:RDD[V]) extends AnyVal {
  def asFeature(variableType:VariableType)(implicit db:DataBuilder[V]):RDD[(Feature,Long)] = {
    v.map(v => StdFeature.from(null, variableType, v)).zipWithIndex
  }
}