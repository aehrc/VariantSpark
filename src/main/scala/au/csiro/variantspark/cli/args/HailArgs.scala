package au.csiro.variantspark.cli.args

import org.kohsuke.args4j.Option
import au.csiro.pbdava.ssparkle.spark.SparkApp
import is.hail.HailContext

/**
 * Common options and properties for applications using Hail.
 */
trait HailArgs extends SparkApp {

  override def createConf = super.createConf
    .set("spark.sql.files.openCostInBytes", "53687091200") // 50GB : min for hail 
    .set("spark.sql.files.maxPartitionBytes", "53687091200") // 50GB : min for hail 

  @Option(name="-mp", required=false, usage="Min partition to use for input dataset(default=spark.default.pararellism)"
      , aliases=Array("--min-partitions"))
  val minPartitions: Int = -1
 
  lazy val hc = HailContext(sc)
  lazy val actualMinPartitions = if (minPartitions > 0) minPartitions else  sc.defaultParallelism 
 
}