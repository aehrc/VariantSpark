package au.csiro.variantspark.cli

import java.io.FileInputStream
import java.io.ObjectInputStream

import scala.collection.JavaConverters._

import org.apache.commons.lang3.builder.ToStringBuilder
import org.apache.spark.Logging
import org.kohsuke.args4j.Option

import au.csiro.pbdava.ssparkle.common.arg4j.AppRunner
import au.csiro.pbdava.ssparkle.common.arg4j.TestArgs
import au.csiro.pbdava.ssparkle.common.utils.LoanUtils
import au.csiro.sparkle.common.args4j.ArgsApp
import au.csiro.variantspark.cmd.EchoUtils._
import au.csiro.variantspark.cmd.Echoable
import au.csiro.variantspark.utils.IndexedRDDFunction._
import au.csiro.variantspark.utils.VectorRDDFunction._
import au.csiro.variantspark.utils.defRng
import com.github.tototoshi.csv.CSVWriter
import au.csiro.variantspark.algo.RandomForestModel
import au.csiro.variantspark.cli.args.FeatureSourceArgs
import au.csiro.pbdava.ssparkle.spark.SparkUtils

class AnalyzeRFCmd extends ArgsApp with FeatureSourceArgs with Logging with TestArgs {

  // input options
  @Option(name="-im", required=true, usage="Path to input model", aliases=Array("--input-model"))
  val inputModel:String = null

  @Option(name="-ob", required=false, usage="Path to output importance", aliases=Array("--output-oob"))
  val outputOobError:String = null

  @Option(name="-obt", required=false, usage="Path to output importance", aliases=Array("--output-oob-per-tree"))
  val outputOobPerTree:String = null
  
  @Option(name="-oi", required=false, usage="Path to output importance", aliases=Array("--output-importance"))
  val outputImportance:String = null
  
  @Option(name="-oti", required=false, usage="Path to output importance", aliases=Array("--output-top-importance"))
  val outputTopImportance:String = null

  @Option(name="-otin", required=false, usage="Path to output importance", aliases=Array("--output-top-importance-number"))
  val outputTopImportanceNumber:Int = 100

  @Override
  def testArgs = Array("-if", "data/chr22_1000.vcf", "-im", "target/ch22-model.ser", 
      "-ob", "target/ch22-oob.csv",
      "-obt", "target/ch22-oob-tree.csv",
      "-oi", "target/ch22-imp.csv", 
      "-oti", "target/ch22-top-imp.csv")
      
  @Override
  def run():Unit = {
    logInfo("Running with params: " + ToStringBuilder.reflectionToString(this))
    echo(s"Analyzing random forrest model")
    
    val rfModel  = LoanUtils.withCloseable(new ObjectInputStream(new FileInputStream(inputModel))) { objIn =>
      objIn.readObject().asInstanceOf[RandomForestModel[_]]
    }  
    echo(s"Model: ${rfModel}") 
  
    if (outputOobError != null) {
      echo(s"Writing oob errors to : ${outputOobError}") 
      LoanUtils.withCloseable(CSVWriter.open(outputOobError)) { writer =>
        writer.writeRow(List("treeNo","oob"))
        rfModel.oobErrors.zipWithIndex.map(_.swap).map(_.productIterator.toSeq).foreach(writer.writeRow)
      }
    }  

    if (outputOobPerTree != null) {
      echo(s"Writing per tree oob : ${outputOobPerTree}") 
      val samples = source.sampleNames
      LoanUtils.withCloseable(CSVWriter.open(outputOobPerTree)) { writer =>
        writer.writeRow(samples)
        rfModel.members.map(m => m.oobIndexs.zip(m.oobPred).toMap)
          .map(m => Range(0,samples.size).map(i => m.getOrElse(i, null))).foreach(writer.writeRow)
      }
    }
    
    if (outputImportance != null) {
      echo(s"Writing per tree importance to: ${outputImportance}") 
      LoanUtils.withCloseable(CSVWriter.open(outputImportance)) { writer =>
        rfModel.trees.map(_.variableImportance()).map(_.toSeq.map(t => t._1 + ":" + t._2)).foreach(writer.writeRow)
      }
    }  
      
    if (outputTopImportance != null) {
      echo(s"Writing top  per tree importance to: ${outputTopImportance}") 
      val topImportantVariables = rfModel.normalizedVariableImportance().toSeq.sortBy(-_._2).take(outputTopImportanceNumber).map(_._1).sorted
      val index = SparkUtils.withBroadcast(sc)(topImportantVariables.toSet) { br_indexes => 
        source.features().zipWithIndex().filter(t => br_indexes.value.contains(t._2)).map({case (f,i) => (i, f.label)}).collectAsMap()
      }  
      //TODO: Need to add mapping to the original names
      
      LoanUtils.withCloseable(CSVWriter.open(outputTopImportance)) { writer =>
        writer.writeRow(topImportantVariables.map(index.apply))
        rfModel.trees.map(_.variableImportance()).map(vi => topImportantVariables.map(i => vi.getOrElse(i, null))).foreach(writer.writeRow)
      }
    }  
  }  
}

object AnalyzeRFCmd  {
  def main(args:Array[String]) {
    AppRunner.mains[AnalyzeRFCmd](args)
  }
}
