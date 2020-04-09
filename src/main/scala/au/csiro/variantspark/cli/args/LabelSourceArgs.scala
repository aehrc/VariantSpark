package au.csiro.variantspark.cli.args

import org.kohsuke.args4j.{Option => ArgsOption}
import au.csiro.variantspark.cmd.Echoable
import au.csiro.variantspark.input.CsvLabelSource
import org.apache.hadoop.fs.FileSystem

trait LabelSourceArgs extends SparkArgs with Echoable {

  @ArgsOption(name = "-ff", required = false, usage = "Path to feature file",
    aliases = Array("--feature-file"))
  val featuresFile: String = null

  @ArgsOption(name = "-fc", required = false, usage = "Name of the feature column",
    aliases = Array("--feature-column"))
  val featureColumn: String = null

  lazy val labelSource = {
    implicit val hadoopConf = sc.hadoopConfiguration
    echo(s"Loading labels from: ${featuresFile}, column: ${featureColumn}")
    new CsvLabelSource(featuresFile, featureColumn)
  }

}
