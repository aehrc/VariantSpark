package au.csiro.variantspark.test

import au.csiro.variantspark.data.VariableType
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import au.csiro.variantspark.data.Feature
import au.csiro.variantspark.input.FeatureSource
import au.csiro.variantspark.data.FeatureBuilder

class TestFeatureSource(val samples: Seq[(String, List[String])], variableType:VariableType, fb:FeatureBuilder[_])(implicit sc: SparkContext) extends FeatureSource {
  def features:RDD[Feature] = sc.parallelize(samples.map(s =>fb.from(s._1, variableType, s._2)))
  def sampleNames: List[String] = Range(0, samples.head._2.size).map("s_" + _).toList
}
