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
import org.apache.spark.mllib.linalg.Vectors
import au.csiro.variantspark.input.CsvLabelSource
import au.csiro.variantspark.cmd.Echoable
import au.csiro.pbdava.ssparkle.common.utils.Logging
import org.apache.commons.lang3.builder.ToStringBuilder
import au.csiro.variantspark.cmd.EchoUtils._
import au.csiro.pbdava.ssparkle.common.utils.LoanUtils
import com.github.tototoshi.csv.CSVWriter
import au.csiro.pbdava.ssparkle.common.arg4j.TestArgs
import org.apache.hadoop.fs.FileSystem
import au.csiro.variantspark.algo.DecisionTree
import org.apache.commons.math3.stat.correlation.PearsonsCorrelation
import au.csiro.pbdava.ssparkle.spark.SparkUtils
import au.csiro.pbdava.ssparkle.common.utils.ReusablePrintStream
import au.csiro.variantspark.algo.RandomForestCallback
import au.csiro.variantspark.utils.VectorRDDFunction._
import au.csiro.variantspark.input.CsvFeatureSource
import au.csiro.variantspark.algo.RandomForestParams
import au.csiro.variantspark.data.BoundedOrdinalVariable
import au.csiro.pbdava.ssparkle.common.utils.Timer
import au.csiro.variantspark.utils.defRng
import au.csiro.variantspark.input.ParquetFeatureSource
import au.csiro.variantspark.algo.RandomForest
import au.csiro.variantspark.utils.IndexedRDDFunction._
import java.io.ObjectOutputStream
import java.io.FileOutputStream
import org.apache.hadoop.conf.Configuration
import au.csiro.variantspark.utils.HdfsPath
import au.csiro.pbdava.ssparkle.common.utils.CSVUtils
import au.csiro.variantspark.cli.args.FeatureSourceArgs
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix
import org.apache.spark.mllib.linalg.distributed.MatrixEntry
import org.apache.commons.math3.util.MathArrays
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.SaveMode
import au.csiro.variantspark.algo.To100ImportanceNormalizer
import au.csiro.variantspark.algo.RawVarImportanceNormalizer
import it.unimi.dsi.util.XorShift1024StarRandomGenerator
import au.csiro.variantspark.cli.args.ImportanceArgs

class NullImportanceCmd
    extends ArgsApp with SparkApp with FeatureSourceArgs with ImportanceArgs with Echoable
    with Logging with TestArgs {

  @Option(name = "-pn", required = false, usage = "Number of permutations to generate (def = 30)",
    aliases = Array("--n-permutations"))
  val nPermutations: Int = 30

  @Option(name = "-pnv", required = false,
    usage = "Number of top important variables from each permutations to include."
      + " Use `0` for all variables (def = 0)",
    aliases = Array("--permutations-n-variables"))
  val permutationsVarLimit: Int = 0

  @Option(name = "-ivo", required = false,
    usage = "Variable type ordinal with this number of levels (def = 3)",
    aliases = Array("--input-var-ordinal"))
  val varOrdinalLevels: Int = 3

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

  // random forrest options

  @Option(name = "-rn", required = false,
    usage = "RandomForest: number of trees to build (def=20)", aliases = Array("--rf-n-trees"))
  val nTrees: Int = 20

  @Option(name = "-rmt", required = false, usage = "RandomForest: mTry(def=sqrt(<num-vars>))",
    aliases = Array("--rf-mtry"))
  val rfMTry: Long = -1L

  @Option(name = "-rmtf", required = false, usage = "RandomForest: mTry fraction",
    aliases = Array("--rf-mtry-fraction"))
  val rfMTryFraction: Double = Double.NaN

  @Option(name = "-ro", required = false, usage = "RandomForest: estimate oob (def=no)",
    aliases = Array("--rf-oob"))
  val rfEstimateOob: Boolean = false
  @Option(name = "-rre", required = false,
    usage = "RandomForest: [DEPRICATED] randomize equal gini recursion is on by default now",
    aliases = Array("--rf-randomize-equal"))
  val rfRandomizeEqual: Boolean = false
  @Option(name = "-rsf", required = false,
    usage = "RandomForest: sample with no replacement (def=1.0 for bootstrap  else 0.6666)",
    aliases = Array("--rf-subsample-fraction"))
  val rfSubsampleFraction: Double = Double.NaN

  @Option(name = "-rsn", required = false,
    usage = "RandomForest: sample with no replacement (def=false -- bootstrap)",
    aliases = Array("--rf-sample-no-replacement"))
  val rfSampleNoReplacement: Boolean = false

  @Option(name = "-rbs", required = false, usage = "RandomForest: batch size (def=10))",
    aliases = Array("--rf-batch-size"))
  val rfBatchSize: Int = 10

  @Option(name = "-sr", required = false, usage = "Random seed to use (def=<random>)",
    aliases = Array("--seed"))
  val randomSeed: Long = defRng.nextLong

  override def testArgs: Array[String] =
    Array("-if", "data/chr22_1000.vcf", "-ff", "data/chr22-labels.csv", "-fc", "22_16051249",
      "-ro", "-of", "target/null-importances.csv", "-sr", "13", "-pn", "5", "-v", "-ivb", "-ovn",
      "raw")

  override def run(): Unit = {
    implicit val fs: FileSystem = FileSystem.get(sc.hadoopConfiguration)
    implicit val hadoopConf: Configuration = sc.hadoopConfiguration

    logDebug(s"Running with filesystem: ${fs}, home: ${fs.getHomeDirectory}")
    logInfo("Running with params: " + ToStringBuilder.reflectionToString(this))
    echo(s"Computing ${nPermutations} variable importance permutations with random forest")

    val dataLoadingTimer = Timer()
    echo(s"Loaded rows: ${dumpList(featureSource.sampleNames)}")
    val inputData = featureSource.features.zipWithIndex().cache()
    val totalVariables = inputData.count()
    val variablePreview =
      inputData.map({ case (f, i) => f.label }).take(defaultPreviewSize).toList
    echo(s"Loaded variables: ${dumpListHead(variablePreview, totalVariables)},"
        + s" took: ${dataLoadingTimer.durationInSec}")
    echoDataPreview()

    echo(s"Loading labels from: ${featuresFile}, column: ${featureColumn}")
    val labelSource = new CsvLabelSource(featuresFile, featureColumn)
    val labels = labelSource.getLabels(featureSource.sampleNames)
    echo(s"Loaded labels: ${dumpList(labels.toList)}")

    // discover variable type
    // for now assume it's ordered factor with provided number of levels
    echo(s"Assumed ordinal variable with ${varOrdinalLevels} levels")
    // TODO (Feature): Add autodiscovery
    val dataType = BoundedOrdinalVariable(varOrdinalLevels)

    val permutationRng = new XorShift1024StarRandomGenerator(randomSeed)

    // For now do it in a loop
    val iterationImportances = Range(0, nPermutations).map { pn =>
      // TODO: need to actually permutate the labels
      // we can do it in place as a permutatino of a permutation is still a permutation
      // although it might be better to actually get the permutation as oder of indexes
      echo(s"Running permutation ${pn}")
      MathArrays.shuffle(labels, permutationRng)
      verbose(s"The permutation is: ${labels.toList}")
      echo(s"Training random forest with trees: ${nTrees} (batch size:  ${rfBatchSize})")
      echo(s"Random seed is: ${randomSeed}")
      val treeBuildingTimer = Timer()
      val rf = new RandomForest(RandomForestParams(oob = rfEstimateOob, seed = randomSeed,
          bootstrap = !rfSampleNoReplacement, subsample = rfSubsampleFraction,
          nTryFraction = if (rfMTry > 0) rfMTry.toDouble / totalVariables else rfMTryFraction))
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
                + s" totalTime: ${totalTime / 1000.0} s,"
                + s" avg timePerTree: ${totalTime / (1000.0 * totalTrees)} s")
          echo(
              s"Last build trees: ${nTrees}, time: ${elapsedTimeMs} ms,"
                + s" timePerTree: ${elapsedTimeMs / nTrees} ms")

        }
      }

      val result = rf.batchTrain(trainingData, labels, nTrees, rfBatchSize)

      echo(
          s"Random forest oob accuracy: ${result.oobError},"
            + s" took: ${treeBuildingTimer.durationInSec} s")

      val topImportantVariables = limitVariables(
          result.normalizedVariableImportance(importanceNormalizer).toSeq, permutationsVarLimit)
      (pn, topImportantVariables)
    }

    // now I need to somehow join the output
    // essentially need to transpose a sparse matrix
    // this seem to be available on a SparkML coordinate matrix
    // actually I do not need to to transpose just build it transposed and
    // convert to indexed row matrix

    val importanceMatrix = new CoordinateMatrix(
        sc.parallelize(
            iterationImportances
              .flatMap(ii => ii._2.map(vi => new MatrixEntry(vi._1, ii._1, vi._2)))
              .toSeq))
      .toIndexedRowMatrix()

    val mappedOutput = importanceMatrix.rows
      .map(k => (k.index, k.vector))
      .join(inputData.map({ case (f, i) => (i, f.label) }))
      .sortByKey()
      .map(_._2)

    val permutationImpSchema = StructType(Seq(StructField("variable", StringType, false))
        ++ Range(0, nPermutations).map(p => StructField(s"perm_${p}", DoubleType, true)))

    val df =
      spark.createDataFrame(mappedOutput.map(r => Row.merge(Row(r._2), Row(r._1.toArray: _*))),
        permutationImpSchema)
    val outputDf = if (nOuputParitions > 0) df.repartition(nOuputParitions) else df
    outputDf.write.mode(SaveMode.Overwrite).option("header", true).csv(outputFile)

  }
}

object NullImportanceCmd {
  def main(args: Array[String]) {
    AppRunner.mains[NullImportanceCmd](args)
  }
}
