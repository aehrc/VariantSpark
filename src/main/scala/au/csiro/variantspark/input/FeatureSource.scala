package au.csiro.variantspark.input

import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag
import au.csiro.variantspark.data.VariableType
import au.csiro.variantspark.data.ContinuousVariable
import au.csiro.variantspark.data.OrdinalVariable
import au.csiro.variantspark.data.Feature


trait FeatureSource {
  def sampleNames:List[String]
  def features: RDD[Feature] 
}


