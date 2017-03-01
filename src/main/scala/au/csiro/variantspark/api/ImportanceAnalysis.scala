package au.csiro.variantspark.api

import au.csiro.variantspark.input.FeatureSource
import au.csiro.variantspark.input.LabelSource
import au.csiro.variantspark.algo.RandomForestParams
import au.csiro.variantspark.algo.ByteRandomForest
import au.csiro.variantspark.data.BoundedOrdinal
import au.csiro.pbdava.ssparkle.spark.SparkUtils
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap
import scala.collection.JavaConversions._



class ImportanceAnalysis(val sqlContext:SQLContext, val featureSource:FeatureSource, 
      val labelSource:LabelSource, 
      val rfParams: RandomForestParams, val nTrees:Int, val rfBatchSize:Int, varOrdinalLevels:Int) {
      
  private def sc = featureSource.features().sparkContext
  private lazy val inputData = featureSource.features().zipWithIndex().cache()
  
  val variableImportanceSchema = StructType(Seq(StructField("variable",StringType,true),StructField("importance",DoubleType,true )))
  
  lazy val rfModel = {
    val dataType = BoundedOrdinal(varOrdinalLevels)
    val labels = labelSource.getLabels(featureSource.sampleNames)
    val rf = new ByteRandomForest(rfParams)
    val traningData = inputData.map{ case (f, i) => (f.values, i)}
    rf.batchTrain(traningData, dataType, labels, nTrees, rfBatchSize)    
  }
  
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
    
    val index = SparkUtils.withBroadcast(featureSource.features().sparkContext)(topImportantVariableIndexes) { br_indexes => 
      inputData.filter(t => br_indexes.value.contains(t._2)).map({case (f,i) => (i, f.label)}).collectAsMap()
    }
    
    topImportantVariables.map({ case (i, importance) => (index(i), importance)})
  } 
}


object ImportanceAnalysis {
 
  def apply(featureSource:FeatureSource, labelSource:LabelSource, rfParams: RandomForestParams = RandomForestParams(), nTrees:Int = 500, rfBatchSize:Int = 100, varOrdinalLevels:Int = 3)
        (implicit vsContext:VSContext) = new ImportanceAnalysis(vsContext.sqlContext, featureSource, labelSource, rfParams, nTrees, rfBatchSize, varOrdinalLevels)  
}