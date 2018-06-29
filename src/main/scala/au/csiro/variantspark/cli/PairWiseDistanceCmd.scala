package au.csiro.variantspark.cli

import java.io.File

import scala.Range

import org.apache.commons.lang3.builder.ToStringBuilder
import au.csiro.pbdava.ssparkle.common.utils.Logging
import org.apache.spark.annotation.Private
import org.kohsuke.args4j.Option

import au.csiro.pbdava.ssparkle.common.arg4j.AppRunner
import au.csiro.pbdava.ssparkle.common.arg4j.TestArgs
import au.csiro.pbdava.ssparkle.common.utils.LoanUtils
import au.csiro.sparkle.common.args4j.ArgsApp
import au.csiro.variantspark.algo.PairwiseOperation
import au.csiro.variantspark.cli.args.FeatureSourceArgs
import au.csiro.variantspark.algo.PairwiseOperation
import au.csiro.variantspark.algo.metrics.EuclideanPairwiseMetric
import au.csiro.variantspark.algo.metrics.ManhattanPairwiseMetric
import au.csiro.variantspark.algo.metrics.BitwiseAndPairwiseRevMetric
import au.csiro.variantspark.algo.metrics.MultiPairwiseRevMetric
import au.csiro.variantspark.algo.metrics.SharedAltAlleleCount
import au.csiro.variantspark.algo.metrics.AtLeastOneSharedAltAlleleCount
import au.csiro.pbdava.ssparkle.common.utils.CSVUtils


class PairWiseDistanceCmd extends ArgsApp with FeatureSourceArgs with Logging with TestArgs {

  @Option(name="-of", required=true, usage="Path to output distance file", aliases=Array("--output-file"))
  val outputFile:String = null

  @Option(name="-m", required=false, usage="Metric to use, one of: euclidean, manhattan, invBitAnd, invMul",
        aliases=Array("--metric"))
  val metricName:String = "euclidean"

  
  @Override
  def testArgs = Array("-if", "data/chr22_1000.vcf", 
      "-of", "target/ch22-disc.csv", "-v",
      "-m", "manhattan"
      )
      
      
  def  buildMetricFromName(metricName:String): PairwiseOperation = {
    metricName match {
      case "euclidean" => EuclideanPairwiseMetric
      case "manhattan" => ManhattanPairwiseMetric
      case "invBitAnd" => BitwiseAndPairwiseRevMetric
      case "invMul" => MultiPairwiseRevMetric
      case "sharedAltCount" => SharedAltAlleleCount
      case "anySharedAltCount" => AtLeastOneSharedAltAlleleCount
      case _ => throw new IllegalArgumentException(metricName)
    }
  }
      
  @Override
  def run():Unit = {
    logInfo("Running with params: " + ToStringBuilder.reflectionToString(this))
    val metric = buildMetricFromName(metricName)
    echo(s"Calculating pair wise distance: ${metric}")
    val data = featureSource.features().map(_.values)
    echoDataPreview()
    val noOfSamples = data.first.length
    val resultAsMatrix = metric.compute(data).toMatrix
    val sampleNames = featureSource.sampleNames
    CSVUtils.withFile(new File(outputFile)) { writer =>
      writer.writeRow("" :: sampleNames)
      // since the matrix is symmetric does not matter that we output columns as rows
      Range(0, noOfSamples).foreach(i => writer.writeRow(sampleNames(i) :: resultAsMatrix(::,i).toArray.toList))
    }   
  }  
}

object PairWiseDistanceCmd  {
  def main(args:Array[String]) {
    AppRunner.mains[PairWiseDistanceCmd](args)
  }
}
