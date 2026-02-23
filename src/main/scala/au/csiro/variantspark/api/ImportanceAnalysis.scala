package au.csiro.variantspark.api

import java.util

import au.csiro.pbdava.ssparkle.spark.SparkUtils
import au.csiro.variantspark.algo.{RandomForest, RandomForestModel, RandomForestParams}
import au.csiro.variantspark.data.{Feature}
import au.csiro.variantspark.input.{FeatureSource, LabelSource}
import it.unimi.dsi.fastutil.longs.{Long2DoubleOpenHashMap, Long2LongOpenHashMap}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.types.{DoubleType, StringType, LongType, StructField, StructType}

import scala.collection.JavaConverters._

/**
  * A class to represent an instance of the Importance Analysis
  *
  * @constructor Create a new `Importance Analysis` by specifying the parameters listed below
  * @param sqlContext    The SQL context.
  * @param featureSource The feature source.
  * @param rfModel The trained random forest model
  * @example class ImportanceAnalysis(featureSource, labelSource, nTrees = 1000)
  */
class ImportanceAnalysis(val sqlContext: SQLContext, val rfModel: RandomForestModel,
    val indexedFeatures: RDD[(Feature, Long)]) {
  private def sc = indexedFeatures.sparkContext

  /** Secondary constructor for backward compatibility.
    * Uses FeatureIndexer to deterministically index features.
    * Prefer the primary constructor with pre-indexed data from TrainResult.
    */
  def this(sqlContext: SQLContext, featureSource: FeatureSource, rfModel: RandomForestModel) = {
    this(sqlContext, rfModel, FeatureIndexer.index(featureSource.features))
  }

  val variableImportanceSchema: StructType =
    StructType(Seq(StructField("variable", StringType, true),
        StructField("importance", DoubleType, true)))

  val variableImportanceWithSplitCountSchema: StructType =
    StructType(Seq(StructField("variant_id", StringType, true),
        StructField("importance", DoubleType, true), StructField("splitCount", LongType, true)))

  private lazy val br_normalizedVariableImportance = {
    val indexImportance = rfModel.normalizedVariableImportance()
    sc.broadcast(
        new Long2DoubleOpenHashMap(
            indexImportance.asInstanceOf[Map[java.lang.Long, java.lang.Double]].asJava))
  }

  private lazy val br_variableImportance = {
    val indexImportance = rfModel.variableImportance
    sc.broadcast(
        new Long2DoubleOpenHashMap(
            indexImportance.asInstanceOf[Map[java.lang.Long, java.lang.Double]].asJava))
  }

  private lazy val br_splitCounts = {
    val splitCounts = rfModel.variableSplitCount
    sc.broadcast(
        new Long2LongOpenHashMap(
            splitCounts.asInstanceOf[Map[java.lang.Long, java.lang.Long]].asJava))
  }

  def variableImportance(normalized: Boolean = false): DataFrame = {
    val local_br_variableImportance = if (!normalized) {
      br_variableImportance
    } else {
      br_normalizedVariableImportance
    }
    val local_br_splitCounts = br_splitCounts

    val importanceRDD = indexedFeatures.map({
      case (f, i) =>
        Row(f.label, local_br_variableImportance.value.get(i), local_br_splitCounts.value.get(i))
    })
    sqlContext.createDataFrame(importanceRDD, variableImportanceWithSplitCountSchema)
  }

  def importantVariables(nTopLimit: Int = 100,
      normalized: Boolean = false): Seq[(String, Double)] = {
    val topImportantVariables = if (!normalized) {
      rfModel.variableImportance.toSeq.sortBy(-_._2).take(nTopLimit)
    } else {
      rfModel.normalizedVariableImportance().toSeq.sortBy(-_._2).take(nTopLimit)
    }
    val topImportantVariableIndexes = topImportantVariables.map(_._1).toSet

    val index =
      SparkUtils.withBroadcast(sc)(topImportantVariableIndexes) { br_indexes =>
        indexedFeatures
          .filter(t => br_indexes.value.contains(t._2))
          .map({ case (f, i) => (i, f.label) })
          .collectAsMap()
      }

    topImportantVariables.map({ case (i, importance) => (index(i), importance) })
  }

  def importantVariablesJavaMap(nTopLimit: Int = 100,
      normalized: Boolean = false): util.Map[String, Double] = {
    val impVarMap =
      collection.mutable.Map(importantVariables(nTopLimit, normalized).toMap.toSeq: _*)
    impVarMap.map { case (k, v) => k -> double2Double(v) }
    impVarMap.asJava
  }
}

object ImportanceAnalysis {

  /** Creates an ImportanceAnalysis using pre-indexed features from training.
    * This is the preferred factory method - it reuses the same indexed RDD
    * that was used during training, ensuring consistent index-to-label mapping.
    */
  def apply(rfModel: RandomForestModel, indexedFeatures: RDD[(Feature, Long)])(
      implicit vsContext: SqlContextHolder): ImportanceAnalysis = {
    new ImportanceAnalysis(vsContext.sqlContext, rfModel, indexedFeatures)
  }

  /** Creates an ImportanceAnalysis by re-indexing features.
    * For backward compatibility - prefer the overload that accepts pre-indexed data.
    */
  def apply(featureSource: FeatureSource, rfModel: RandomForestModel)(
      implicit vsContext: SqlContextHolder): ImportanceAnalysis = {
    new ImportanceAnalysis(vsContext.sqlContext, featureSource, rfModel)
  }
}
