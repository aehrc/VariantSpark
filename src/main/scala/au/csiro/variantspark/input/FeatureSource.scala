package au.csiro.variantspark.input

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import java.util.Arrays


case class VectorFeature(label:String, values: Vector)

case class Feature(label:String, values: Array[Byte]) extends Equals {
  require(label != null)
  require(values != null)
  
  def toVector = new VectorFeature(label, Vectors.dense(values.map(_.toDouble)))

  override def equals(other: Any) = {
    other match {
      case that: Feature => label == that.label && values.toSeq == that.values.toSeq
      case _ => false
    }
  }

  override def hashCode() = {
    val prime = 41
    prime * (prime + label.hashCode) + values.toSeq.hashCode
  }
  
  
}


trait FeatureSource {
  def sampleNames:List[String]
  def features():RDD[Feature]
}