package au.csiro.variantspark.cli.args

import org.kohsuke.args4j.{Option => ArgsOption}
import au.csiro.variantspark.cmd.Echoable
import au.csiro.variantspark.input.CsvLabelSource
import org.apache.hadoop.fs.FileSystem
import au.csiro.variantspark.algo.RandomForestModel
import org.json4s.jackson.Serialization
import org.json4s.NoTypeHints
import java.io.ObjectOutputStream
import au.csiro.variantspark.utils.HdfsPath
import au.csiro.pbdava.ssparkle.common.utils.LoanUtils
import java.io.OutputStreamWriter
import au.csiro.variantspark.external.ModelConverter
import org.json4s._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.{write, writePretty}
import java.io.OutputStreamWriter
import au.csiro.variantspark.external.Forest
import au.csiro.variantspark.external.ModelConverter
import org.apache.hadoop.conf.Configuration

trait ModelOutputArgs extends SparkArgs with Echoable {

  @ArgsOption(name = "-om", required = false, usage = "Path to model file",
    aliases = Array("--model-file"))
  val modelFile: String = null

  @ArgsOption(name = "-omf", required = false,
    usage = "Format of the model file, one of: `json`, `java` (def=`java`)",
    aliases = Array("--model-file-format"))
  val modelFileFormat: String = "java"

  def requiresFullIndex = modelFile != null

  def saveModelJson(rfModel: RandomForestModel, variableIndex: Map[Long, String]) {
    implicit val hadoopConf: Configuration = sc.hadoopConfiguration
    implicit val formats = Serialization.formats(NoTypeHints)
    LoanUtils.withCloseable(new OutputStreamWriter(HdfsPath(modelFile).create())) { objectOut =>
      writePretty(new ModelConverter(variableIndex).toExternal(rfModel), objectOut)
    }
  }

  def saveModelJava(rfModel: RandomForestModel, variableIndex: Map[Long, String]) {
    implicit val hadoopConf: Configuration = sc.hadoopConfiguration
    LoanUtils.withCloseable(new ObjectOutputStream(HdfsPath(modelFile).create())) { objectOut =>
      objectOut.writeObject(rfModel)
    }
  }

  def saveModel(rfModel: RandomForestModel, variableIndex: Map[Long, String]) {
    if (modelFile != null) {
      echo(s"Saving random forest model as `${modelFileFormat}` to: ${modelFile}")
      modelFileFormat match {
        case "java" => saveModelJava(rfModel, variableIndex)
        case "json" => saveModelJson(rfModel, variableIndex)
        case _ =>
          throw new IllegalArgumentException("Unrecognized model format type: " + modelFileFormat)
      }

    }
  }
}
