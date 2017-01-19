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
import org.apache.spark.serializer.JavaSerializer
import au.csiro.variantspark.cli.args.SparkArgs
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap

class AnalyzeRFCmd extends ArgsApp with SparkArgs with Echoable with Logging with TestArgs {

  // input options
  @Option(name="-im", required=true, usage="Path to input model", aliases=Array("--input-model"))
  val inputModel:String = null

  @Option(name="-ii", required=false, usage="Path to input variabel index file", aliases=Array("--input-idnex"))
  val inputIndex:String = null

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
  def testArgs = Array("-ii", "target/ch22-idx.ser", "-im", "target/ch22-model.ser", 
      "-ob", "target/ch22-oob.csv",
      "-obt", "target/ch22-oob-tree.csv",
      "-oi", "target/ch22-imp.csv", 
      "-oti", "target/ch22-top-imp.csv")
      
      
  lazy val variableIndex = LoanUtils.withCloseable(new ObjectInputStream(new FileInputStream(inputIndex))) { objIn =>
      objIn.readObject().asInstanceOf[Long2ObjectOpenHashMap[String]]
  }  
   
  @Override
  def run():Unit = {
    logInfo("Running with params: " + ToStringBuilder.reflectionToString(this))
    echo(s"Analyzing random forrest model")
    
    
    //NOTE: There is some wirdness going on here with the classloaded
    //So I am using the Spark JavaSerializer to the the rihght one
    val javaSerializer = new JavaSerializer(conf)
    val si = javaSerializer.newInstance()
    
    val rfModel  = LoanUtils.withCloseable(new FileInputStream(inputModel)) { in =>
      si.deserializeStream(in).readObject().asInstanceOf[RandomForestModel[_]]
    }  
    
    echo(s"Loaded model of size: ${rfModel.size}") 
  
    if (outputOobError != null) {
      echo(s"Writing oob errors to : ${outputOobError}") 
      LoanUtils.withCloseable(CSVWriter.open(outputOobError)) { writer =>
        writer.writeRow(List("treeNo","oob"))
        rfModel.oobErrors.zipWithIndex.map(_.swap).map(_.productIterator.toSeq).foreach(writer.writeRow)
      }
    }  

//    if (outputOobPerTree != null) {
//      echo(s"Writing per tree oob : ${outputOobPerTree}") 
//      val samples = source.sampleNames
//      LoanUtils.withCloseable(CSVWriter.open(outputOobPerTree)) { writer =>
//        writer.writeRow(samples)
//        rfModel.members.map(m => m.oobIndexs.zip(m.oobPred).toMap)
//          .map(m => Range(0,samples.size).map(i => m.getOrElse(i, null))).foreach(writer.writeRow)
//      }
//    }
    
    if (outputImportance != null) {
      echo(s"Writing per tree importance to: ${outputImportance}") 
      LoanUtils.withCloseable(CSVWriter.open(outputImportance)) { writer =>
        rfModel.trees.map(_.variableImportance()).map(_.toSeq.map(t => t._1 + ":" + t._2)).foreach(writer.writeRow)
      }
    }  
      
    if (outputTopImportance != null) {
      echo(s"Writing top  per tree importance to: ${outputTopImportance}") 
      val topImportantVariables = rfModel.normalizedVariableImportance().toSeq.sortBy(-_._2).take(outputTopImportanceNumber).map(_._1).sorted
      
      LoanUtils.withCloseable(CSVWriter.open(outputTopImportance)) { writer =>
        writer.writeRow(topImportantVariables.map(variableIndex.get))
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
