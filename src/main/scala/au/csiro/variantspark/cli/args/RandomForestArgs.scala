package au.csiro.variantspark.cli.args

import org.kohsuke.args4j.Option

trait RandomForestArgs {

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

  @Option(name = "-rmd", required = false,
    usage = "Maximum depth a tree should go after which it should stop splitting.",
    aliases = Array("--rf-max-depth"))
  val rfMaxDepth: Int = Int.MaxValue

  @Option(name = "-rmns", required = false,
    usage = "Minimum node size after which tree should stop splitting.",
    aliases = Array("--rf-min-node-size"))
  val rfMinNodeSize: Int = 1
}
