package au.csiro.variantspark.cli.args

import org.kohsuke.args4j.Option
import au.csiro.pbdava.ssparkle.spark.SparkApp
import org.apache.spark.rdd.RDD

trait SparkArgs extends SparkApp {

  @Option(name = "-sp", required = false, usage = "Spark parallelism (def=<default-spark-par>)",
    aliases = Array("--spark-par"))
  val sparkPar = 0

  def textFile(inputFile: String): RDD[String] =
    sc.textFile(inputFile, if (sparkPar > 0) sparkPar else sc.defaultParallelism)

}
