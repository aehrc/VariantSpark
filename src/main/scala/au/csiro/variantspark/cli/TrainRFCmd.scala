package au.csiro.variantspark.cli

import au.csiro.pbdava.ssparkle.common.arg4j.{AppRunner, TestArgs}
import au.csiro.pbdava.ssparkle.common.utils.Logging
import au.csiro.sparkle.common.args4j.ArgsApp
import au.csiro.variantspark.utils.defRng
import au.csiro.variantspark.cli.args.{
  FeatureSourceArgs,
  LabelSourceArgs,
  ModelOutputArgs,
  RandomForestArgs
}
import au.csiro.variantspark.cmd.EchoUtils._
import au.csiro.variantspark.cmd.Echoable
import org.apache.commons.lang3.builder.ToStringBuilder
import org.apache.hadoop.conf.Configuration
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.{RandomForest => SparkForest}
import org.apache.spark.mllib.tree.model.{RandomForestModel => SparkForestModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.{JavaSerializer, SerializerInstance}
import org.kohsuke.args4j.Option

import scala.collection._
import scala.util.Random

class TrainRFCmd
    extends ArgsApp with LabelSourceArgs with RandomForestArgs with FeatureSourceArgs
    with ModelOutputArgs with Echoable with Logging with TestArgs {

  @Option(name = "-lf", required = false, usage = "Path to label file",
    aliases = Array("--label-file"))
  val labelFile: String = null

  @Option(name = "-lc", required = false, usage = "Label file column name",
    aliases = Array("--label-column"))
  val labelColumn: String = null

  val javaSerializer = new JavaSerializer(conf)
  val si: SerializerInstance = javaSerializer.newInstance()

  override def testArgs: Array[String] =
    Array("-im", "file.model", "-if", "file.data", "-of", "outputpredictions.file")

  override def run(): Unit = {
    implicit val hadoopConf: Configuration = sc.hadoopConfiguration

    // echo(s"Loading labels from: ${featuresFile}, column: ${featureColumn}")
    val labels: List[String] =
      if (featuresFile != null) {
        val labFile =
          spark.read.format("csv").option("header", "true").load(featuresFile)
        val labCol = labFile.select(featureColumn).rdd.map(_(0)).collect.toList
        labCol.map(_.toString)
      } else {
        // val labelSource = new CsvLabelSource(featuresFile, featureColumn)
        // val labels = labelSource.getLabels(featureSource.sampleNames)
        val dummyLabels =
          List("blue", "brown", "black", "green", "yellow", "grey")
        // val featureCount = 2000
        val featureCount = featureSource.features.count.toInt
        val phenoLabelIndex = Range(0, featureCount).toList
          .map(_ => dummyLabels(Random.nextInt.abs % dummyLabels.length))
        phenoLabelIndex
      }
    /* write map of labels to file for lookup after prediction
      allows human readable labels in results
     */
    val label2pt = labels.toSet.zipWithIndex.toMap
    val pt2label = label2pt.map(l => l.swap)
    sc.parallelize(pt2label.toSeq).saveAsTextFile(modelFile + ".labelMap")
    echo(s"Loaded labels from file: ${labels.toSet}")
    echo(s"Loaded labels: ${dumpList(labels)}")

    val labPts = labels zip featureSource.features.collect map {
      case (label, feat) =>
        LabeledPoint(label2pt(label).toDouble, feat.valueAsVector)
    }
    val labPtsRDD = sc.parallelize(labPts)
    val catInfo = immutable.Map[Int, Int]()
    val numClasses = labels.toSet.size
    val numTrees = scala.Option(nTrees).getOrElse(5)
    val subsetStrat = "auto"
    val impurity = "gini"
    val maxDepth: Int = scala.Option(rfMaxDepth).getOrElse(30)
    val maxBins = 32
    val intSeed = defRng.nextLong.toInt
    val sparkRFModel: SparkForestModel = SparkForest.trainClassifier(labPtsRDD, numClasses,
      catInfo, numTrees, subsetStrat, impurity, scala.math.min(maxDepth, 30), maxBins, intSeed)
    println("running train cmd")
    logInfo("Running with params: " + ToStringBuilder.reflectionToString(this))
    echo(s"Analyzing random forest model")
    echo(s"Using spark RF Model: ${sparkRFModel.toString}")
    echo(s"Using labels: ${labels}")
    echo(s"Loaded rows: ${dumpList(featureSource.sampleNames)}")

    if (modelFile != null) {
      sparkRFModel.save(sc, modelFile)
    }
  }
}

object TrainRFCmd {
  def main(args: Array[String]) {
    AppRunner.mains[TrainRFCmd](args)
  }
}
