package au.csiro.variantspark.cli

import java.io.File

import au.csiro.pbdava.ssparkle.common.arg4j.AppRunner
import au.csiro.pbdava.ssparkle.common.utils.CSVUtils
import au.csiro.pbdava.ssparkle.spark.SparkApp
import au.csiro.sparkle.common.args4j.ArgsApp
import au.csiro.variantspark.input.{VCFFeatureSource, VCFSource}
import org.kohsuke.args4j.Option

class VcfToLabels extends ArgsApp with SparkApp {

  @Option(name = "-if", required = false, usage = "This is input files",
    aliases = Array("--input-file"))
  val inputFile: String = null

  @Option(name = "-of", required = false, usage = "Output file", aliases = Array("--output-file"))
  val outputFile: String = null

  @Option(name = "-l", required = false)
  val limit: Int = 10

  override def run(): Unit = {
    val vcfSource = VCFSource(sc.textFile(inputFile))
    val header = vcfSource.header
    val version = vcfSource.version
    println(header)
    println(version)
    val source = VCFFeatureSource(vcfSource, imputationStrategy = "none")
    val columns = source.features.take(limit)
    CSVUtils.withFile(new File(outputFile)) { writer =>
      writer.writeRow("" :: columns.map(_.label).toList)
      source.sampleNames.zipWithIndex.foreach {
        case (row, i) =>
          writer.writeRow(row :: columns.map(_.valueAsByteArray(i)).toList)
      }
    }

  }
}

object VcfToLabels {
  def main(args: Array[String]) {
    AppRunner.mains[VcfToLabels](args)
  }
}
