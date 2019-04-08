package au.csiro.variantspark.api

import au.csiro.pbdava.ssparkle.spark.SparkUtils
import au.csiro.variantspark.algo.{RandomForest, RandomForestParams}
import au.csiro.variantspark.data.BoundedOrdinalVariable
import au.csiro.variantspark.input.{FeatureSource, LabelSource}
import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import au.csiro.variantspark.algo.RandomForest

/**
  * A class to represent an instance of the Importance Analysis
  *
  * @constructor Create a new `Importance Analysis` by specifying the parameters listed below
  *
  * @param sqlContext    The SQL context.
  * @param featureSource The feature source.
  * @param labelSource  The label source.
  * @param rfParams The Random Forest parameters.
  * @param nTrees The number of Decision Trees.
  * @param rfBatchSize The batch size of the Random Forest
  * @param varOrdinalLevels The level of ordinal
  *
  * @example class ImportanceAnalysis(featureSource, labelSource, nTrees = 1000)
  */

class ImportanceAnalysis(val sqlContext:SQLContext, val featureSource:FeatureSource, 
      val labelSource:LabelSource, 
      val rfParams: RandomForestParams, val nTrees:Int, val rfBatchSize:Int, varOrdinalLevels:Int) {
      
  private def sc = featureSource.features.sparkContext
  private lazy val inputData = featureSource.features.zipWithIndex().cache()
  
  val variableImportanceSchema = StructType(Seq(StructField("variable",StringType,true),StructField("importance",DoubleType,true )))
  
  lazy val rfModel = {
    val labels = labelSource.getLabels(featureSource.sampleNames)
    val rf = new RandomForest(rfParams)
    rf.batchTrain(inputData, labels, nTrees, rfBatchSize)
  }

  val oobError: Double  = rfModel.oobError

  private lazy val br_normalizedVariableImportance = {
    val indexImportance = rfModel.normalizedVariableImportance()   
    sc.broadcast(new Long2DoubleOpenHashMap(indexImportance.asInstanceOf[Map[java.lang.Long, java.lang.Double]]))
  }
 
  def variableImportance = {
    val local_br_normalizedVariableImportance = br_normalizedVariableImportance
    val importanceRDD  = inputData.map({ case (f,i) =>  Row(f.label, local_br_normalizedVariableImportance.value.get(i))})
    sqlContext.createDataFrame(importanceRDD, variableImportanceSchema)
  }
  
  def importantVariables(nTopLimit:Int = 100) = {
      // build index for names
    val topImportantVariables = rfModel.normalizedVariableImportance().toSeq.sortBy(-_._2).take(nTopLimit)
    val topImportantVariableIndexes = topImportantVariables.map(_._1).toSet
    
    val index = SparkUtils.withBroadcast(featureSource.features.sparkContext)(topImportantVariableIndexes) { br_indexes => 
      inputData.filter(t => br_indexes.value.contains(t._2)).map({case (f,i) => (i, f.label)}).collectAsMap()
    }
    
    topImportantVariables.map({ case (i, importance) => (index(i), importance)})
  }

  def importantVariablesJavaMap(nTopLimit:Int = 100) = {
    val impVarMap = collection.mutable.Map(importantVariables(nTopLimit).toMap.toSeq: _*)
    impVarMap.map{ case (k, v) => k -> double2Double(v) }
    impVarMap.asJava
  }
}

object ImportanceAnalysis {

  val defaultRFParams = RandomForestParams()
  
  def apply(featureSource:FeatureSource, labelSource:LabelSource, nTrees:Int = 1000, 
        mtryFraction:Option[Double] = None, oob:Boolean = true,
        seed: Option[Long] = None, batchSize:Int = 100, varOrdinalLevels:Int = 3)
        (implicit vsContext:SqlContextHolder): ImportanceAnalysis = {
    
    new ImportanceAnalysis(vsContext.sqlContext, featureSource, labelSource, 
    rfParams = RandomForestParams(
          nTryFraction = mtryFraction.getOrElse(defaultRFParams.nTryFraction), 
          seed =  seed.getOrElse(defaultRFParams.seed), 
          oob = oob
      ),
      nTrees = nTrees,
      rfBatchSize = batchSize, 
      varOrdinalLevels = varOrdinalLevels
    )
  }
  
  def fromParams(featureSource:FeatureSource, labelSource:LabelSource, 
          rfParams: RandomForestParams, nTrees:Int = 1000, 
         batchSize:Int = 100, varOrdinalLevels:Int = 3)
        (implicit vsContext:SqlContextHolder): ImportanceAnalysis = {
    
    new ImportanceAnalysis(vsContext.sqlContext, featureSource, labelSource, 
      rfParams = rfParams,
      nTrees = nTrees,
      rfBatchSize = batchSize, 
      varOrdinalLevels = varOrdinalLevels
    )
  }
  
}