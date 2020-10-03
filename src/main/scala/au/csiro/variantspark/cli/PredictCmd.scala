package au.csiro.variantspark.cli

import java.io.{FileInputStream, ObjectInputStream}

import au.csiro.pbdava.ssparkle.common.arg4j.{AppRunner, TestArgs}
import au.csiro.pbdava.ssparkle.common.utils._
import au.csiro.pbdava.ssparkle.spark.SparkApp
import au.csiro.sparkle.common.args4j.ArgsApp
import au.csiro.variantspark.algo.{RandomForestModel, _}
import au.csiro.variantspark.cli.args.{
  FeatureSourceArgs,
  ImportanceArgs,
  ModelOutputArgs,
  RandomForestArgs
}
import au.csiro.variantspark.cmd.EchoUtils._
import au.csiro.variantspark.cmd.Echoable
import au.csiro.variantspark.input.CsvLabelSource
import au.csiro.variantspark.utils.defRng
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap
import org.apache.commons.lang3.builder.ToStringBuilder
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.serializer.JavaSerializer
import org.kohsuke.args4j.{Option => ArgsOption}

class PredictCmd
    extends ArgsApp with FeatureSourceArgs with Echoable with SparkApp with ImportanceArgs
    with RandomForestArgs with ModelOutputArgs with Logging with TestArgs {
  lazy val variableIndex: Long2ObjectOpenHashMap[String] =
    LoanUtils.withCloseable(new ObjectInputStream(new FileInputStream(inputIndex))) { objIn =>
      objIn.readObject().asInstanceOf[Long2ObjectOpenHashMap[String]]
    }
  /* e.g run with
   * ./bin/variant-spark predict -if data/chr22_1000.vcf -ff data/chr22-labels.csv
   * -fc 22_16051249 -om target/ch22-model.json -omf json -sr 13 -ii data/hipster_labels.txt
   *  -im data/ch22-model.java
   */
  @ArgsOption(name = "-im", required = true, usage = "Path to input model",
    aliases = Array("--input-model"))
  val inputModel: String = null
  @ArgsOption(name = "-ii", required = false, usage = "Path to input variable index file",
    aliases = Array("--input-index"))
  val inputIndex: String = null
  @ArgsOption(name = "-ff", required = true, usage = "Path to feature file",
    aliases = Array("--feature-file"))
  val featuresFile: String = null
  @ArgsOption(name = "-fc", required = true, usage = "Name of the feature column",
    aliases = Array("--feature-column"))
  val featureColumn: String = null
  @ArgsOption(name = "-of", required = false, usage = "Path to output file (def = stdout)",
    aliases = Array("--output-file"))
  val outputFile: String = null
  @ArgsOption(name = "-on", required = false,
    usage = "The number of top important variables to include in output."
      + " Use `0` for all variables. (def=20)",
    aliases = Array("--output-n-variables"))
  val nVariables: Int = 20
  @ArgsOption(name = "-od", required = false,
    usage = "Include important variables data in output file (def=no)",
    aliases = Array("--output-include-data"))
  val includeData: Boolean = false
  @ArgsOption(name = "-sr", required = false, usage = "Random seed to use (def=<random>)",
    aliases = Array("--seed"))
  val randomSeed: Long = defRng.nextLong

  override def testArgs: Array[String] =
    Array("-if", "data/chr22_1000.vcf", "-ff", "data/chr22-labels.csv", "-fc", "22_16051249",
      "-ovn", "raw", "-on", "1988", "-rn", "1000", "-rbs", "100", "-ic", "-om",
      "target/ch22-model.json", "-omf", "json", "-sr", "13", "-v", "-io", "-ii",
      "target/ch22-idx.ser", "-im", "target/ch22-model.ser", "-if", "data/chr22_1000.vcf", "-ob",
      "target/ch22-oob.csv", "-obt", "target/ch22-oob-tree.csv", "-oi", "target/ch22-imp.csv",
      "-oti", "target/ch22-top-imp.csv", """{"separator":":"}""")

  override def run(): Unit = {
    logInfo("Running with params: " + ToStringBuilder.reflectionToString(this))
    echo(s"Analyzing random forest model")

    val javaSerializer = new JavaSerializer(conf)
    val si = javaSerializer.newInstance()

    val rfModel =
      LoanUtils.withCloseable(new FileInputStream(inputModel)) { in =>
        si.deserializeStream(in).readObject().asInstanceOf[RandomForestModel]
      }

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

    echo(s"Loading labels from: ${featuresFile}, column: ${featureColumn}")
    val labelSource = new CsvLabelSource(featuresFile, featureColumn)
    val labels = labelSource.getLabels(featureSource.sampleNames)
    echo(s"Loaded labels: ${dumpList(labels.toList)}")
    echo(s"Random seed is: ${randomSeed}")
    echo(s"Loaded model of size: ${rfModel.size}")
    echo("Running prediction analysis")
    val preds = rfModel.predictProb(featureSource.features.zipWithIndex())
    echo(s"Unique predictions: ${dumpList(preds.distinct.toList)}")
    echo(s"Predictions: ${dumpList(preds.toList)}")

    CSVUtils.withStream(
        if (outputFile != null) HdfsPath(outputFile).create() else ReusablePrintStream.stdout) {
      writer =>
        val header =
          labels ::: (if (includeData) featureSource.sampleNames else Nil)
        writer.writeRow(header)
        writer.writeAll(preds.map(_=>_))
    }

  }
}

object PredictCmd {
  def main(args: Array[String]) {
    AppRunner.mains[PredictCmd](args)
  }
}
