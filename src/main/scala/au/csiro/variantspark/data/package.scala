package au.csiro.variantspark
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.{Vectors,Vector}

package object data {
  
  
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
  
  implicit def toFeatueConverter[V](v:RDD[V]):ToFeature[V] = new ToFeature[V](v)
}