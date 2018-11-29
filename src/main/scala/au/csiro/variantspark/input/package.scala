package au.csiro.variantspark

import org.apache.spark.mllib.linalg.{ Vector, Vectors }
import au.csiro.variantspark.data.VariableType
import au.csiro.variantspark.data.Feature
import au.csiro.variantspark.data.FeatureBuilder

package object input {

//  implicit object CanRepresentVector extends FeatureBuilder[Vector] with Serializable {
//    def from(label: String, l: List[String]) = VectorFeature(label, Vectors.dense(l.map(_.toDouble).toArray))
//    def from(label: String, l: Array[Byte]) = VectorFeature(label, Vectors.dense(l.map(_.toDouble).toArray))
//    def toListOfStrings(v: Vector): List[String] = v.toArray.toList.map(_.toString)
//  }
//
//  implicit object CanRepresentByteArray extends FeatureBuilder[Array[Byte]] with Serializable {
//    def from(label: String, l: List[String]) = ByteArrayFeature(label, l.map(_.toByte).toArray)
//    def from(label: String, l: Array[Byte]) = ByteArrayFeature(label, l)
//    def toListOfStrings(v: Array[Byte]): List[String] = v.toList.map(_.toString)
//  }

  implicit case object VectorFeatureBuilder extends FeatureBuilder[Vector] {
    def from(label: String, variableType: VariableType, l: List[String]): Feature = {
      VectorFeature(label, variableType, Vectors.dense(l.map(_.toDouble).toArray))
    }
    def from(label: String, variableType: VariableType, v:Vector): Feature = {
      VectorFeature(label, variableType, v)
    }    
  }

  implicit case object ByteArrayFeatureBuilder extends FeatureBuilder[Array[Byte]] {
    def from(label: String, variableType: VariableType, l: List[String]): Feature = {
      ByteArrayFeature(label, variableType, l.map(_.toByte).toArray)
    }
  
    def from(label: String, variableType: VariableType, v:Array[Byte]): Feature = {
      ByteArrayFeature(label, variableType, v)
    }
  }
}