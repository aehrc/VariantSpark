package au.csiro.variantspark.cli
import au.csiro.pbdava.ssparkle.common.arg4j.{AppRunner, TestArgs}
import au.csiro.pbdava.ssparkle.common.utils.{Logging, Timer}
import au.csiro.pbdava.ssparkle.spark.{SparkApp, SparkUtils}
import au.csiro.sparkle.common.args4j.ArgsApp
import au.csiro.variantspark.algo.{
  DefTreeRepresentationFactory,
  RandomForest,
  RandomForestCallback,
  RandomForestParams
}
import au.csiro.variantspark.cli.args._
import au.csiro.variantspark.cmd.EchoUtils._
import au.csiro.variantspark.cmd.Echoable
import au.csiro.variantspark.input.CsvLabelSource
import au.csiro.variantspark.utils.defRng
import org.apache.commons.lang3.builder.ToStringBuilder
import org.apache.hadoop.conf.Configuration
import org.apache.spark.serializer.{JavaSerializer, SerializerInstance}
import org.kohsuke.args4j.Option

class TrainRFCmd
    extends ArgsApp with SparkApp with LabelSourceArgs with RandomForestArgs with ImportanceArgs
    with FeatureSourceArgs with ModelOutputArgs with Echoable with Logging with TestArgs {

  @Option(name = "-of", required = false, usage = "Path to output file (def = stdout)",
    aliases = Array("--output-file"))
  val outputFile: String = null

  @Option(name = "-im", required = false, usage = "Path to input model",
    aliases = Array("--input-model"))
  val inputModel: String = null

  @Option(name = "-ii", required = false, usage = "Path to input variable index file",
    aliases = Array("--input-index"))
  val inputIndex: String = null

  @Option(name = "-lf", required = false, usage = "Path to label file",
    aliases = Array("--label-file"))
  val labelFile: String = null

  @Option(name = "-lc", required = false, usage = "Label file column name",
    aliases = Array("--label-column"))
  val labelColumn: String = null

  @Option(name = "-sr", required = false, usage = "Random seed to use (def=<random>)",
    aliases = Array("--seed"))
  val randomSeed: Long = defRng.nextLong

  @Option(name = "-on", required = false,
    usage = "The number of top important variables to include in output."
      + " Use `0` for all variables. (def=20)",
    aliases = Array("--output-n-variables"))
  val nVariables: Int = 20

  @Option(name = "-od", required = false,
    usage = "Include important variables data in output file (def=no)",
    aliases = Array("--output-include-data"))
  val includeData: Boolean = false

  val javaSerializer = new JavaSerializer(conf)
  val si: SerializerInstance = javaSerializer.newInstance()

  override def testArgs: Array[String] =
    Array("-im", "file.model", "-if", "file.data", "-of", "outputpredictions.file")

  override def run(): Unit = {
    implicit val hadoopConf: Configuration = sc.hadoopConfiguration

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

    println("running train cmd")
    logInfo("Running with params: " + ToStringBuilder.reflectionToString(this))
    echo(s"Analyzing random forest model")
    echo(s"Using spark RF Model: ${result.toString}")
    echo(s"Using labels: ${labels}")
    echo(s"Loaded rows: ${dumpList(featureSource.sampleNames)}")

    saveModel(result, index.toMap)
    echo(s"inputFile: ${inputFile}")
  }
}

object TrainRFCmd {
  def main(args: Array[String]) {
    AppRunner.mains[TrainRFCmd](args)
  }
}
