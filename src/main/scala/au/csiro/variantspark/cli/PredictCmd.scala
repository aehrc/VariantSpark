package au.csiro.variantspark.cli

import java.io.FileInputStream

import au.csiro.pbdava.ssparkle.common.arg4j.{AppRunner, TestArgs}
import au.csiro.pbdava.ssparkle.common.utils._
import au.csiro.pbdava.ssparkle.spark.SparkApp
import au.csiro.sparkle.common.args4j.ArgsApp
import au.csiro.variantspark.algo.RandomForestModel
import au.csiro.variantspark.cli.args.FeatureSourceArgs
import au.csiro.variantspark.cmd.EchoUtils._
import au.csiro.variantspark.cmd.Echoable
import au.csiro.variantspark.utils.HdfsPath
import org.apache.commons.lang3.builder.ToStringBuilder
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.serializer.JavaSerializer
import org.kohsuke.args4j.{Option => ArgsOption}

class PredictCmd
    extends ArgsApp with FeatureSourceArgs with Echoable with SparkApp with Logging
    with TestArgs {

  /* e.g run with
   * ./bin/variant-spark predict -if data/chr22_1000.vcf -ff data/chr22-labels.csv
   * -fc 22_16051249 -om target/ch22-model.json -omf json -sr 13 -ii data/hipster_labels.txt
   *  -im data/ch22-model.java
   */
  @ArgsOption(name = "-im", required = true, usage = "Path to input model",
    aliases = Array("--input-model"))
  val inputModel: String = null

  @ArgsOption(name = "-of", required = false, usage = "Path to output file (def = stdout)",
    aliases = Array("--output-file"))
  val outputFile: String = null

  override def testArgs: Array[String] =
    Array("-if", "data/chr22_1000.vcf", "-im", "data/ch22-model.ser", "-v")

  override def run(): Unit = {
    implicit val fs: FileSystem = FileSystem.get(sc.hadoopConfiguration)

    implicit val hadoopConf: Configuration = sc.hadoopConfiguration
    logDebug(s"Running with filesystem: ${fs}, home: ${fs.getHomeDirectory}")
    logInfo("Running with params: " + ToStringBuilder.reflectionToString(this))

    echo(s"Running random forest prediction")

    val javaSerializer = new JavaSerializer(conf)
    val si = javaSerializer.newInstance()

    val rfModel =
      LoanUtils.withCloseable(new FileInputStream(inputModel)) { in =>
        si.deserializeStream(in).readObject().asInstanceOf[RandomForestModel]
      }

    echo(s"Loaded model of size: ${rfModel.size}")

    val dataLoadingTimer = Timer()
    echo(s"Loaded rows: ${dumpList(featureSource.sampleNames)}")

    val inputData = featureSource.features.zipWithIndex().cache()
    val totalVariables = inputData.count()

    val variablePreview = inputData.map(_._1.label).take(defaultPreviewSize).toList
    echo(s"Loaded variables: ${dumpListHead(variablePreview, totalVariables)},"
        + s" took: ${dataLoadingTimer.durationInSec}")
    echoDataPreview()

    val classProbabilities: Array[Array[Double]] =
      rfModel.predictProb(inputData)
    val predictionRows = (featureSource.sampleNames zip classProbabilities)
      .map {
        case (sampleName, classProb) =>
          sampleName :: classProb.indices.maxBy(classProb) :: Nil ::: classProb.toList
      }

    CSVUtils.withStream(if (outputFile != null) {
      HdfsPath(outputFile).create()
    } else ReusablePrintStream.stdout) { writer =>
      val header =
        List("sample", "class") ::: Range(0, rfModel.labelCount).map(ci => s"p_${ci}").toList
      writer.writeRow(header)
      writer.writeAll(predictionRows)
    }
  }
}

object PredictCmd {
  def main(args: Array[String]) {
    AppRunner.mains[PredictCmd](args)
  }
}
