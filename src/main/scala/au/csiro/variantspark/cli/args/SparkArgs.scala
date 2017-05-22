package au.csiro.variantspark.cli.args

import org.kohsuke.args4j.Option
import au.csiro.pbdava.ssparkle.spark.SparkApp


trait SparkArgs extends SparkApp {

  @Option(name="-sp", required=false, usage="Spark parallelism (def=<default-spark-par>)", aliases=Array("--spark-par"))
  val sparkPar = 0
}