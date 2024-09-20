package au.csiro.variantspark.api

import java.util

import java.io.{BufferedWriter, OutputStreamWriter}
import au.csiro.variantspark.algo.{
  DefTreeRepresentationFactory,
  RandomForest,
  RandomForestModel,
  RandomForestParams,
  TreeFeature
}
import au.csiro.variantspark.input.FeatureSource
import au.csiro.variantspark.external.ModelConverter
import au.csiro.variantspark.utils.HdfsPath
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.hadoop.conf.Configuration
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.writePretty
import org.json4s.{Formats, NoTypeHints}
import scala.collection.mutable.ListBuffer
import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap
import scala.collection.JavaConverters._

class ExportModel(rfModel: RandomForestModel, featureSource: FeatureSource) {
  val sc: SparkContext = featureSource.features.sparkContext

  private lazy val br_variableImportance = {
    val indexImportance = rfModel.variableImportance
    sc.broadcast(
        new Long2DoubleOpenHashMap(
            indexImportance.asInstanceOf[Map[java.lang.Long, java.lang.Double]].asJava))
  }

  def toJson(jsonFilename: String, resolveVarNames: Boolean, batchSize: Int): Unit = {
    println(s"Saving model")
    val inputData: RDD[TreeFeature] =
      DefTreeRepresentationFactory.createRepresentation(featureSource.features.zipWithIndex())
    implicit val hadoopConf: Configuration = sc.hadoopConfiguration
    implicit val formats: Formats = Serialization.formats(NoTypeHints)

    val local_br_variableImportance = br_variableImportance

    val hdfsPath = new HdfsPath(jsonFilename)
    val outputStream = hdfsPath.create()
    val writer = new BufferedWriter(new OutputStreamWriter(outputStream))

    try {
      writer.write("{\n\"params\" : ")
      writer.write(writePretty(rfModel.params))
      writer.write(",\n\"trees\": [\n")

      val totalTrees = rfModel.members.length

      for (batchStart <- 0 until totalTrees by batchSize) {
        val batchEnd = Math.min(batchStart + batchSize, totalTrees)
        val treeBatch = rfModel.members.slice(batchStart, batchEnd)

        val importantFeaturesBatch = if (resolveVarNames) {
          inputData
            .mapPartitions { partition =>
              val impVariableSet = local_br_variableImportance.value.keySet
              partition
                .filter(f => impVariableSet.contains(f.index))
                .map(f => (f.index, f.label))
            }
            .collect()
            .toMap
        } else {
          Map.empty[Long, String]
        }

        val modelConverter = new ModelConverter(importantFeaturesBatch)

        treeBatch.zipWithIndex.foreach {
          case (tree, index) =>
            val externalTree = modelConverter.toExternal(tree)
            writer.write(writePretty(externalTree))
            if (batchStart + index + 1 < totalTrees) writer.write(",\n")
        }

        writer.flush()
      }

      writer.write("\n]")
      if (rfModel.params.oob == true) {
        writer.write(", \n\"oobErrors\" : ")
        writer.write(writePretty(rfModel.oobErrors))
      }
      writer.write("\n}\n")
    } finally {
      writer.close()
      outputStream.close()
      println(s"Model saved successfully to: ${jsonFilename}")
    }
  }
}
