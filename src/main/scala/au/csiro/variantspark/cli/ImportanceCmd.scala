package au.csiro.variantspark.cli

import au.csiro.pbdava.ssparkle.common.arg4j.{AppRunner, TestArgs}
import au.csiro.pbdava.ssparkle.common.utils.{CSVUtils, Logging, ReusablePrintStream, Timer}
import au.csiro.pbdava.ssparkle.spark.{SparkApp, SparkUtils}
import au.csiro.sparkle.common.args4j.ArgsApp
import au.csiro.variantspark.algo.{RandomForest, RandomForestCallback, RandomForestParams, _}
import au.csiro.variantspark.cli.args.{
  FeatureSourceArgs,
  ImportanceArgs,
  ModelOutputArgs,
  RandomForestArgs
}
import au.csiro.variantspark.cmd.EchoUtils._
import au.csiro.variantspark.cmd.Echoable
import au.csiro.variantspark.input.CsvLabelSource
import au.csiro.variantspark.utils.{HdfsPath, defRng}
import org.apache.commons.lang3.builder.ToStringBuilder
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.kohsuke.args4j.Option

class ImportanceCmd
    extends ArgsApp with SparkApp with FeatureSourceArgs with ImportanceArgs with RandomForestArgs
    with ModelOutputArgs with Echoable with Logging with TestArgs {

  @Option(name = "-ff", required = true, usage = "Path to feature file",
    aliases = Array("--feature-file"))
  val featuresFile: String = null

  @Option(name = "-fc", required = true, usage = "Name of the feature column",
    aliases = Array("--feature-column"))
  val featureColumn: String = null

  // output options
  @Option(name = "-of", required = false, usage = "Path to output file (def = stdout)",
    aliases = Array("--output-file"))
  val outputFile: String = null

  @Option(name = "-on", required = false,
    usage = "The number of top important variables to include in output."
      + " Use `0` for all variables. (def=20)",
    aliases = Array("--output-n-variables"))
  val nVariables: Int = 20

  @Option(name = "-od", required = false,
    usage = "Include important variables data in output file (def=no)",
    aliases = Array("--output-include-data"))
  val includeData: Boolean = false

  @Option(name = "-sr", required = false, usage = "Random seed to use (def=<random>)",
    aliases = Array("--seed"))
  val randomSeed: Long = defRng.nextLong

  override def testArgs: Array[String] =
    Array("-if", "data/chr22_1000.vcf", "-ff", "data/chr22-labels.csv", "-fc", "22_16051347",
      "-ovn", "raw", "-on", "1988", "-rn", "1000", "-rbs", "250", "-ic", "-om",
      "target/ch22-model.ser", "-omf", "java", "-sr", "13", "-v", "-io", """{"separator":":"}""",
      "-ro", "-rmtf", "0.1")

  override def run(): Unit = {
    implicit val fs: FileSystem = FileSystem.get(sc.hadoopConfiguration)
    implicit val hadoopConf: Configuration = sc.hadoopConfiguration
    logDebug(s"Running with filesystem: ${fs}, home: ${fs.getHomeDirectory}")
    logInfo("Running with params: " + ToStringBuilder.reflectionToString(this))

    echo(s"Finding  ${nVariables}  most important features using random forest")

    val dataLoadingTimer = Timer()
    echo(s"Loaded rows: ${dumpList(featureSource.sampleNames)}")

    val inputData = DefTreeRepresentationFactory
      .createRepresentation(featureSource.features.zipWithIndex())
      .cache()
    val totalVariables = inputData.count()

    val variablePreview = inputData.map(_.label).take(defaultPreviewSize).toList
    echo(s"Loaded variables: ${dumpListHead(variablePreview, totalVariables)},"
        + s" took: ${dataLoadingTimer.durationInSec}")
    echoDataPreview()

    // if (isVerbose) {
    //  verbose("Representation preview:")
    //  inputData.take(defaultPreviewSize).foreach(f=> verbose(s"${f.label}:
    //  ${f.variableType}:${dumpList(f.valueAsStrings,
    //  longPreviewSize)}(${f.getClass.getName})"))
    // }

    echo(s"Loading labels from: ${featuresFile}, column: ${featureColumn}")
    val labelSource = new CsvLabelSource(featuresFile, featureColumn)
    val labels = labelSource.getLabels(featureSource.sampleNames)
    echo(s"Loaded labels: ${dumpList(labels.toList)}")
    echo(s"Training random forest with trees: ${nTrees} (batch size:  ${rfBatchSize})")
    echo(s"Random seed is: ${randomSeed}")
    val treeBuildingTimer = Timer()
    val rf: RandomForest = new RandomForest(RandomForestParams(oob = rfEstimateOob,
        seed = randomSeed, maxDepth = rfMaxDepth, minNodeSize = rfMinNodeSize,
        bootstrap = !rfSampleNoReplacement, subsample = rfSubsampleFraction,
        nTryFraction = if (rfMTry > 0) rfMTry.toDouble / totalVariables else rfMTryFraction,
        correctImpurity = correctImportance, airRandomSeed = airRandomSeed))

    //
    val trainingData = inputData

    implicit val rfCallback: RandomForestCallback = new RandomForestCallback() {
      var totalTime: Long = 0L
      var totalTrees: Int = 0
      override def onParamsResolved(actualParams: RandomForestParams) {
        echo(s"RF Params: ${actualParams}")
        echo(s"RF Params mTry: ${(actualParams.nTryFraction * totalVariables).toLong}")
      }
      override def onTreeComplete(nTrees: Int, oobError: Double, elapsedTimeMs: Long) {
        totalTime += elapsedTimeMs
        totalTrees += nTrees
        echo(
            s"Finished trees: ${totalTrees}, current oobError: ${oobError},"
              + s" totalTime: ${totalTime / 1000.0} s, "
              + s" avg timePerTree: ${totalTime / (1000.0 * totalTrees)} s")
        echo(
            s"Last build trees: ${nTrees}, time: ${elapsedTimeMs} ms,"
              + s" timePerTree: ${elapsedTimeMs / nTrees} ms")

      }
    }

    val result = rf.batchTrainTyped(trainingData, labels, nTrees, rfBatchSize)

    echo(
        s"Random forest oob accuracy: ${result.oobError},"
          + s" took: ${treeBuildingTimer.durationInSec} s")

    // build index for names
    val allImportantVariables = result.normalizedVariableImportance(importanceNormalizer).toSeq
    val topImportantVariables = limitVariables(allImportantVariables, nVariables)
    val topImportantVariableIndexes = topImportantVariables.map(_._1).toSet

    val variablesToIndex = if (requiresFullIndex) {
      allImportantVariables.map(_._1).toSet
    } else {
      topImportantVariableIndexes
    }

    val index = SparkUtils.withBroadcast(sc)(variablesToIndex) { br_indexes =>
      inputData
        .filter(t => br_indexes.value.contains(t.index))
        .map(f => (f.index, f.label))
        .collectAsMap()
    }

    val varImportance = topImportantVariables.map({
      case (i, importance) => (index(i), importance)
    })

    if (isEcho && outputFile != null) {
      echo("Variable importance preview")
      varImportance
        .take(math.min(math.max(nVariables, defaultPreviewSize), defaultPreviewSize))
        .foreach({ case (label, importance) => echo(s"${label}: ${importance}") })
    }

    val importantVariableData =
      if (includeData) trainingData.collectAtIndexes(topImportantVariableIndexes) else null

    CSVUtils.withStream(
        if (outputFile != null) HdfsPath(outputFile).create() else ReusablePrintStream.stdout) {
      writer =>
        val header =
          List("variable", "importance") ::: (if (includeData) featureSource.sampleNames else Nil)
        writer.writeRow(header)
        writer.writeAll(topImportantVariables.map({
          case (i, importance) =>
            List(index(i), importance) ::: (if (includeData) {
                                              importantVariableData(i).valueAsStrings
                                            } else { Nil })
        }))
    }

    saveModel(result, index.toMap)
  }
}

object ImportanceCmd {
  def main(args: Array[String]) {
    AppRunner.mains[ImportanceCmd](args)
  }
}
