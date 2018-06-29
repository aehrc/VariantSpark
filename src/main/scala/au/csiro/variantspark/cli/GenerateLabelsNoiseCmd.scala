package au.csiro.variantspark.cli

import au.csiro.sparkle.common.args4j.ArgsApp
import au.csiro.sparkle.cmd.CmdApp
import org.kohsuke.args4j.Option
import au.csiro.pbdava.ssparkle.common.arg4j.AppRunner
import au.csiro.pbdava.ssparkle.spark.SparkApp
import collection.JavaConverters._
import au.csiro.variantspark.input.VCFSource
import au.csiro.variantspark.input.VCFFeatureSource
import au.csiro.variantspark.input.HashingLabelSource
import au.csiro.variantspark.algo.WideRandomForest
import org.apache.spark.mllib.linalg.Vectors
import au.csiro.variantspark.input.CsvLabelSource
import au.csiro.variantspark.cmd.Echoable
import au.csiro.pbdava.ssparkle.common.utils.Logging
import org.apache.commons.lang3.builder.ToStringBuilder
import au.csiro.variantspark.cmd.EchoUtils._
import au.csiro.pbdava.ssparkle.common.utils.LoanUtils
import au.csiro.pbdava.ssparkle.common.arg4j.TestArgs
import org.apache.hadoop.fs.FileSystem
import au.csiro.variantspark.algo.WideDecisionTree
import org.apache.commons.math3.stat.correlation.PearsonsCorrelation
import au.csiro.pbdava.ssparkle.spark.SparkUtils
import au.csiro.pbdava.ssparkle.common.utils.ReusablePrintStream
import au.csiro.variantspark.algo.RandomForestCallback
import au.csiro.variantspark.utils.VectorRDDFunction._
import au.csiro.variantspark.input.CsvFeatureSource
import au.csiro.variantspark.algo.RandomForestParams
import au.csiro.variantspark.data.BoundedOrdinal
import au.csiro.pbdava.ssparkle.common.utils.Timer
import au.csiro.variantspark.utils.defRng
import au.csiro.variantspark.input.generate.OrdinalFeatureGenerator
import au.csiro.variantspark.output.ParquetFeatureSink
import au.csiro.variantspark.input.ParquetFeatureSource
import au.csiro.variantspark.input.generate.NoisyEffectLabelGenerator
import java.io.File
import java.util.ArrayList
import java.io.PrintStream
import au.csiro.pbdava.ssparkle.common.utils.CSVUtils

class GenerateLabelsNoiseCmd extends ArgsApp with SparkApp with Echoable with Logging with TestArgs {

  // input options
  @Option(name = "-if", required = true, usage = "Path to input file or directory", aliases = Array("--input-file"))
  val inputFile: String = null

  @Option(name = "-it", required = false, usage = "Input file type, one of: vcf, csv (def=parquet)", aliases = Array("--input-type"))
  val inputType: String = "parquet"

  @Option(name = "-ivo", required = false, usage = "Variable type ordinal with this number of levels (def = 3)", aliases = Array("--input-var-ordinal"))
  val varOrdinalLevels: Int = 3

  // output options

  @Option(name = "-ff", required = true, usage = "Path to feature file", aliases = Array("--feature-file"))
  val featuresFile: String = null

  @Option(name = "-fc", required = true, usage = "Name of the dichotomous response column" , aliases = Array("--feature-column"))
  val featureColumn: String = null

  @Option(name = "-fcc", required = false, usage = "Name of the continuous response column(def=None)", aliases = Array("--feature-continuous-column"))
  val featureContinuousColumn: String = null

  @Option(name = "-fiv", required = false, usage = "Include effect variable data", aliases = Array("--feature-include-variables"))
  val includeEffectVarData: Boolean = false

  // generator options
  @Option(name = "-gfve", required = false, usage = "Fraction of variance explained by the effects (def=0.2)", aliases = Array("--gen-frac-var-explained"))
  val fracVarExplained: Double = 0.2

  @Option(name = "-gct", required = false, usage = "Generator class threshold percentile (def=0.750", aliases = Array("--gen-class-threshold"))
  val classThresholdPercentile: Double = 0.75

  @Option(name = "-gz", required = false, usage = "Generator zero level (def = <factor-levels>/2)", aliases = Array("--gen-zero-level"))
  val zeroLevel: Int = -1

  @Option(name = "-gm", required = false, usage = "Multiply effect contributions", aliases = Array("--gen-multiplicative-effect"))
  val multiplicativeEffects: Boolean = false


  @Option(name = "-ge", required = false, usage = "Generator effects <var-name>:<effect-size> (can be used may times)", aliases = Array("--gen-effect"))
  val effectsDef: ArrayList[String] = new ArrayList()

  // common options

  @Option(name = "-sr", required = false, usage = "Random seed to use (def=<random>)", aliases = Array("--seed"))
  val randomSeed: Long = defRng.nextLong

  // spark related
  @Option(name = "-sp", required = false, usage = "Spark parallelism (def=<default-spark-par>)", aliases = Array("--spark-par"))
  val sparkPar = 0

  @Override
  def testArgs = Array("-if", "target/getds.parquet", "-sp", "4", "-ff", "target/features.csv", "-fc", "resp", "-sr", "133", "-v", "-fcc", "resp_cont", "-fiv",
    "-ge", "v_0:1.0", "-ge", "v_1:1.0", "-gfve", "0.5" , "-gct", "0.66")

  @Override
  def run(): Unit = {
    logInfo("Running with params: " + ToStringBuilder.reflectionToString(this))

    val actualZeroLevel = if (zeroLevel > 0) zeroLevel else varOrdinalLevels / 2
    echo(s"Generating a dichotomous response, zeroLevel: ${actualZeroLevel}, fracVarExplained: ${fracVarExplained}")

    val effects = effectsDef.asScala.map(_.split(":")).map { case Array(v, e) => (v, e.toDouble) }.toMap
    echo(s"Effects: ${effects}")

    verbose(s"Random seed is: ${randomSeed}, sparkPar is: ${sparkPar}")

    echo(s"Loading parquet file: ${inputFile}")
    val featureSource = new ParquetFeatureSource(inputFile)
    echo(s"Loaded rows: ${dumpList(featureSource.sampleNames)}")

    val generator = NoisyEffectLabelGenerator(featureSource)(zeroLevel = actualZeroLevel, effects = effects,
      fractionVarianceExplained = fracVarExplained, classThresholdPercentile = classThresholdPercentile,
      multiplicative = multiplicativeEffects,
      seed = randomSeed)
    echo(s"Saving feature output to: ${featuresFile}, column: ${featureColumn}")

    val labels = generator.getLabels(featureSource.sampleNames)

    echo(s"Continous response mean: ${generator.noisyContinuousStats.mean} , total variance: ${generator.noisyContinuousStats.variance}")

    val effectVarData = if (includeEffectVarData) {
      SparkUtils.withBroadcast(sc)(effects) { br_effects =>
        featureSource.features.filter(f => br_effects.value.contains(f.label)).map(f => (f.label, f.values)).collectAsMap()
      }
    } else Map.empty

    CSVUtils.withFile(new File(featuresFile)) { writer =>
      writer.writeRow(List("", featureColumn) ::: (if (featureContinuousColumn!=null) List(featureContinuousColumn) else Nil) ::: effectVarData.toList.map(_._1))
      val outputColumns = List(featureSource.sampleNames, labels.toList) ::: (if (featureContinuousColumn!=null) List(generator.noisyContinuousResponse.data.toList) else Nil) ::: effectVarData.toList.map(_._2.toList)
      writer.writeAll(outputColumns.transpose)
    }
  }

  def withFileOrStdout(fileName: String)(f: PrintStream => Unit) = {
    LoanUtils.withCloseable(if (fileName != null) new PrintStream(fileName) else
      ReusablePrintStream.stdout)(f)
  }

}

object GenerateLabelsNoiseCmd {
  def main(args: Array[String]) {
    AppRunner.mains[GenerateLabelsNoiseCmd](args)
  }
}
