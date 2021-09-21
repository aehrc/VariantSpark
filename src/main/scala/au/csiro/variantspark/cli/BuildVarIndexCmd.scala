package au.csiro.variantspark.cli

import java.io.{FileOutputStream, ObjectOutputStream}

import au.csiro.pbdava.ssparkle.common.arg4j.{AppRunner, TestArgs}
import au.csiro.pbdava.ssparkle.common.utils.{LoanUtils, Logging}
import au.csiro.sparkle.common.args4j.ArgsApp
import au.csiro.variantspark.cli.args.FeatureSourceArgs
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap
import org.apache.commons.lang3.builder.ToStringBuilder
import org.apache.spark.util.AccumulatorV2
import org.kohsuke.args4j.Option

// TODO: Documentation says the access to the collection should be synchronized
class IndexAccumulatorV2
    extends AccumulatorV2[(Long, String), Long2ObjectOpenHashMap[String]] {

  override val value: Long2ObjectOpenHashMap[String] = new Long2ObjectOpenHashMap[String]()

  override def isZero: Boolean = value.isEmpty

  override def copy(): AccumulatorV2[(Long, String), Long2ObjectOpenHashMap[String]] = {
    val clone = new IndexAccumulatorV2()
    clone.merge(this)
    clone
  }

  override def reset(): Unit = {
    value.clear()
  }
  override def add(v: (Long, String)): Unit = value.put(v._1, v._2)

  override def merge(
      other: AccumulatorV2[(Long, String), Long2ObjectOpenHashMap[String]]): Unit = {
    value.putAll(other.value)
  }
}

class BuildVarIndexCmd extends ArgsApp with FeatureSourceArgs with Logging with TestArgs {

  @Option(name = "-oi", required = true, usage = "Path to output index file",
    aliases = Array("--output-index"))
  val outputIndex: String = null

  override def testArgs: Array[String] =
    Array("-if", "data/chr22_1000.vcf", "-oi", "target/ch22-idx.ser")

  override def run(): Unit = {
    logInfo("Running with params: " + ToStringBuilder.reflectionToString(this))
    echo(s"Building full variable index")
    val indexAccumulator = new IndexAccumulatorV2()
    sc.register(indexAccumulator, "IndexAccumulator")
    featureSource.features
      .zipWithIndex()
      .map(t => (t._2, t._1.label))
      .foreach(indexAccumulator.add)

    val index = indexAccumulator.value

    echo(s"Saving index of ${index.size()} variables to: ${outputIndex}")
    LoanUtils.withCloseable(new ObjectOutputStream(new FileOutputStream(outputIndex))) {
      objectOut => objectOut.writeObject(index)
    }
  }
}

object BuildVarIndexCmd {
  def main(args: Array[String]) {
    AppRunner.mains[BuildVarIndexCmd](args)
  }
}
