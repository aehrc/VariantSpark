package au.csiro.variantspark.input

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors


case class VectorFeature(label:String, values: Vector)
case class Feature(label:String, values: Array[Int]) {
  def toVector = new VectorFeature(label, Vectors.dense(values.map(_.toDouble)))
}


trait FeatureSource {
  def rowNames:List[String]
  def features():RDD[Feature]
}