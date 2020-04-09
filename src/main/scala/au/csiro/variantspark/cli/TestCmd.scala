package au.csiro.variantspark.cli

import au.csiro.sparkle.common.args4j.ArgsApp
import au.csiro.sparkle.cmd.CmdApp
import org.kohsuke.args4j.Option
import au.csiro.pbdava.ssparkle.common.arg4j.AppRunner
import collection.JavaConverters._
import au.csiro.pbdava.ssparkle.common.arg4j.TestArgs
import au.csiro.pbdava.ssparkle.common.utils.LoanUtils._
import org.apache.hadoop.fs.Path
import breeze.linalg.DenseVector
import breeze.linalg.support.CanSlice
import breeze.stats.distributions.Binomial
import breeze.stats.distributions.Bernoulli
import org.apache.spark.util.random.BernoulliSampler
import org.apache.spark.util.random.BernoulliSampler

class TestCmd extends ArgsApp with TestArgs /*with SparkApp*/ {

  @Option(
    name = "-if",
    required = false,
    usage = "This is input files",
    aliases = Array("--input-files"))
  val inputFile: String = null

  @Option(name = "-l", required = false)
  val limit: Int = 0

  @Override
  def testArgs = Array("-l", "10")

  def ec(x: Int*) = {
    println(x)
  }

  @Override
  def run(): Unit = {
    //implicit val fs = FileSystem.get(sc.hadoopConfiguration)
    //logDebug(s"Running with filesystem: ${fs}, home: ${fs.getHomeDirectory}")
    println("Hello Word: " + inputFile + ", " + limit)
    println("Properties: ")
    //System.getProperties().asScala.foreach(k => println(k))
    val labels: DenseVector[Int] = DenseVector.zeros(10)
    println(labels(2, 3, 4).toArray.toList)
    labels(0 to 2)
    println(labels(0 to 2).toArray.toList)
    ec(1, 3, 4)
    ec(Array(4, 5): _*)
    val s = labels(1, Array(4, 5): _*)
    println(s.slices)
    println(s.keySet)

    val cc = labels(List(1, 4))
    println(cc.slices)
    println(cc.toArray)

    val binomialDist = Binomial(100, 1.0 / 100)
    val dist = binomialDist.draw()
    Range(0, 10).foreach(_ => println(binomialDist.draw()))
    val sm = new BernoulliSampler[Int](0.01)
    //sm.sample(items)
  }
}

object TestCmd {
  def main(args: Array[String]) {
    AppRunner.mains[TestCmd](args)
  }
}
