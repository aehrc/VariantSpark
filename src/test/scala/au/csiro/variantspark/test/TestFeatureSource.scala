package au.csiro.variantspark.test

import au.csiro.variantspark.data.VariableType
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import au.csiro.variantspark.data._
import au.csiro.variantspark.input.FeatureSource
import au.csiro.variantspark.data.DataBuilder

class TestFeatureSource(
    val samples: Seq[(String, List[String])],
    variableType: VariableType,
    db: DataBuilder[_])(implicit sc: SparkContext)
    extends FeatureSource {
  def features: RDD[Feature] =
    sc.parallelize(samples.map(s => StdFeature(s._1, variableType, db.from(s._2))))
  def sampleNames: List[String] = Range(0, samples.head._2.size).map("s_" + _).toList
}
