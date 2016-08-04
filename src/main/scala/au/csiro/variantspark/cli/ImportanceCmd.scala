package au.csiro.variantspark.cli

import au.csiro.sparkle.common.args4j.ArgsApp
import au.csiro.sparkle.cmd.CmdApp
import org.kohsuke.args4j.Option
import au.csiro.pbdava.ssparkle.common.arg4j.AppRunner
import au.csiro.pbdava.ssparkle.spark.SparkApp
import collection.JavaConverters._
import au.csiro.variantspark.input.VCFSource
import au.csiro.variantspark.input.VCFFeatureSource
import au.csiro.variantspark.input.HashingLabelSource
import au.csiro.variantspark.algo.WideRandomForest
import org.apache.spark.mllib.linalg.Vectors
import au.csiro.variantspark.input.CsvLabelSource
import au.csiro.variantspark.algo.cmd.Echoable
import org.apache.spark.Logging


class ImportanceCmd extends ArgsApp with SparkApp with Echoable with Logging {

  @Option(name="-if", required=false, usage="This is input files", aliases=Array("--input-file"))
  val inputFile:String = "data/small.vcf"
  
  @Option(name="-ff", required=false, usage="Features file", aliases=Array("--feature-file"))
  val featuresFile:String = "data/small-labels.csv"

  @Option(name="-fc", required=false, usage="Feature column", aliases=Array("--feature-column"))
  val featureColumn:String = "22_16051249"
  
  @Option(name="-l", required=false, usage="Number od features to print")
  val limit:Int = 20

  
  @Override
  def run():Unit = {
   
  
    logInfo("Hello through logging")
    echo("Hello Word: " + inputFile + ", " + limit)
    val vcfSource = VCFSource(sc.textFile(inputFile))
    val header = vcfSource.header
    val version = vcfSource.version  
    verbose("VCF Header: " + header)
    verbose("VCF Version:" + version)
    val source  = VCFFeatureSource(vcfSource)
    echo("Rows: " + source.rowNames)
    
    val labelSource = new CsvLabelSource(featuresFile, featureColumn)
    
    val labels = labelSource.getLabels(source.rowNames)
    echo("Rows:" + labels.toList)  
    val trainLables = labels
    val inputData = source.features().map(_.toVector).zipWithIndex().cache()
    echo("Total variables: " + inputData.count())

    val trainSetWithIndex = inputData.map{ case (f, i) => (f.values, i)}
    val rf = new WideRandomForest()
    val result  = rf.run(trainSetWithIndex, trainLables, 5)    
    // build index for names
    val index = inputData.map({case (f,i) => (i, f.label)}).collectAsMap()
    val variableImportnace = result.variableImportance
    variableImportnace.toSeq.sortBy(-_._2).take(limit).map({ case (i, importance) => (index(i), importance)}).foreach(println)
    echo("Done")
    
  }
}

object ImportanceCmd  {
  def main(args:Array[String]) {
    AppRunner.mains[ImportanceCmd](args)
  }
}
