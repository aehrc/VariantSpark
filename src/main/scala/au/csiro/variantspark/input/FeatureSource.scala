package au.csiro.variantspark.input

import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, DoubleType, StructField, StructType}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import scala.reflect.ClassTag
import au.csiro.variantspark.data.VariableType
import au.csiro.variantspark.data.ContinuousVariable
import au.csiro.variantspark.data.OrdinalVariable
import au.csiro.variantspark.data.Feature

trait FeatureSource {
  def sampleNames: List[String]
  def features: RDD[Feature]

  def head(sqlContext: SQLContext, rowLim: Int = 10, colLim: Int = 10): DataFrame = {
    lazy val sampleNamesStructArr: Array[StructField] =
      sampleNames.take(colLim).map(StructField(_, StringType, true)).toArray
    lazy val featureDFSchema: StructType =
      StructType(Seq(StructField("variant_id", StringType, true)) ++ sampleNamesStructArr)
    val sc = sqlContext.sparkContext

    val slicedFeatureArray: Array[Row] =
      features.take(rowLim).map { f =>
        Row.fromSeq(f.label +: f.valueAsStrings.take(colLim).toSeq)
      }
    val slicedFeatureRDD: RDD[Row] = sc.parallelize(slicedFeatureArray)
    sqlContext.createDataFrame(slicedFeatureRDD, featureDFSchema)
  }
}
