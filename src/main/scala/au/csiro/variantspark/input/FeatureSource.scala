package au.csiro.variantspark.input

import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag



trait Feature[V] {
  def label: String
  def values: V
  def toVector: Feature[Vector]
}

trait CanRepresent[V] {
  def from(label: String, l:List[String]):Feature[V]
  def from(label: String, l:Array[Byte]):Feature[V]
  def toListOfStrings(v:V):List[String]
}

trait FeatureSource {
  def sampleNames:List[String]
  def featuresAs[V](implicit cr:CanRepresent[V]):RDD[Feature[V]]
  def features(): RDD[Feature[Array[Byte]]] = featuresAs[Array[Byte]]
}


case class VectorFeature(label:String, values: Vector) extends Feature[Vector] {
  def toVector =  this
}

case class ByteArrayFeature(label:String, values: Array[Byte]) extends Feature[Array[Byte]] with Equals {
  require(label != null)
  require(values != null)
  
  def toVector = new VectorFeature(label, Vectors.dense(values.map(_.toDouble)))

  override def equals(other: Any) = {
    other match {
      case that: ByteArrayFeature => label == that.label && values.toSeq == that.values.toSeq
      case _ => false
    }
  }

  override def hashCode() = {
    val prime = 41
    prime * (prime + label.hashCode) + values.toSeq.hashCode
  }  
}
