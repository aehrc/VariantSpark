package au.csiro.variantspark.test

import org.apache.spark.sql.SparkSession
import au.csiro.variantspark.cli.VariantSparkApp

abstract class AbstractCmdLineTest {
  import SparkTestUtils._

  def runVariantSpark(cmdLine: String, sessionBuilder: SparkSession.Builder = MasterLocal2) {
    withSessionBuilder(sessionBuilder) { _ => VariantSparkApp.main(cmdLine.split(" ")) }
  }
}
