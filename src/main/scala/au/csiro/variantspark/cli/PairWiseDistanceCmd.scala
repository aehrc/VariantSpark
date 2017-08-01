package au.csiro.variantspark.cli

import java.io.File

import scala.Range

import org.apache.commons.lang3.builder.ToStringBuilder
import au.csiro.pbdava.ssparkle.common.utils.Logging
import org.apache.spark.annotation.Private
import org.kohsuke.args4j.Option

import com.github.tototoshi.csv.CSVWriter

import au.csiro.pbdava.ssparkle.common.arg4j.AppRunner
import au.csiro.pbdava.ssparkle.common.arg4j.TestArgs
import au.csiro.pbdava.ssparkle.common.utils.LoanUtils
import au.csiro.sparkle.common.args4j.ArgsApp
import au.csiro.variantspark.algo.PairwiseDistance
import au.csiro.variantspark.cli.args.FeatureSourceArgs
import au.csiro.variantspark.algo.PairwiseDistance
import au.csiro.variantspark.algo.PairwiseMetric
import au.csiro.variantspark.algo.EucledianPairwiseMetric
import au.csiro.variantspark.algo.ManhattanPairwiseMetric
import au.csiro.variantspark.algo.BitwiseAndPairwiseRevMetric
import au.csiro.variantspark.algo.MultiPairwiseRevMetric


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
      
      
  def  buildMetricFromName(metricName:String): PairwiseMetric = {
    metricName match {
      case "euclidean" => EucledianPairwiseMetric
      case "manhattan" => ManhattanPairwiseMetric
      case "invBitAnd" => BitwiseAndPairwiseRevMetric
      case "invMul" => MultiPairwiseRevMetric
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
    val resultAsMatrix = PairwiseDistance.upperTriangWithDiagToMatrix(new PairwiseDistance(metric).compute(data), noOfSamples)
    val sampleNames = featureSource.sampleNames
    LoanUtils.withCloseable(CSVWriter.open(new File(outputFile))) { writer =>
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
