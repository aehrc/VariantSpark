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
import org.apache.commons.lang3.builder.ToStringBuilder
import au.csiro.variantspark.algo.cmd.EchoUtils._
import au.csiro.pbdava.ssparkle.common.utils.LoanUtils
import com.github.tototoshi.csv.CSVWriter

class ImportanceCmd extends ArgsApp with SparkApp with Echoable with Logging {

  @Option(name="-if", required=false, usage="This is input files", aliases=Array("--input-file"))
  val inputFile:String = "data/small.vcf"
  
  @Option(name="-ff", required=false, usage="Features file", aliases=Array("--feature-file"))
  val featuresFile:String = "data/small-labels.csv"

  @Option(name="-fc", required=false, usage="Feature column", aliases=Array("--feature-column"))
  val featureColumn:String = "22_16051249"
  
  @Option(name="-f", required=false, usage="Number od features to print", aliases=Array("--n-features"))
  val limit:Int = 20

  @Option(name="-t", required=false, usage="Number of tree to build", aliases=Array("--n-trees") )
  val nTrees:Int = 5

  @Option(name="-of", required=false, usage="Output file", aliases=Array("--output-file") )
  val outputFile:String = "target/output.csv"

  
  @Override
  def run():Unit = {
    logInfo("Running with params: " + ToStringBuilder.reflectionToString(this))
   
    echo(s"Finding  ${limit}  most important features using random forest")

    echo(s"Loading header from: ${inputFile}")
    val vcfSource = VCFSource(sc.textFile(inputFile))
    verbose(s"VCF Version: ${vcfSource.version}")
    verbose(s"VCF Header: ${vcfSource.header}")    
    val source  = VCFFeatureSource(vcfSource)
    echo(s"Loaded rows: ${dumpList(source.rowNames)}")
     
    echo(s"Loading labels from: ${featuresFile}, column: ${featureColumn}")
    val labelSource = new CsvLabelSource(featuresFile, featureColumn)
    val labels = labelSource.getLabels(source.rowNames)
    echo(s"Loaded labels: ${dumpList(labels.toList)}")
    
    echo(s"Loading features from: ${inputFile}")
    val inputData = source.features().map(_.toVector).zipWithIndex().cache()
    val totalVariables = inputData.count()
    val variablePerview = inputData.map({case (f,i) => f.label}).take(defaultPreviewSize).toList
    echo(s"Loaded variables: ${dumpListHead(variablePerview, totalVariables)}")    

    if (isVerbose) {
      verbose("Data preview:")
      source.features().take(defaultPreviewSize).foreach(f=> verbose(s"${f.label}:${dumpList(f.values.toList, longPreviewSize)}"))
    }
    
    echo(s"Training random forest - trees: ${nTrees}")  
    val rf = new WideRandomForest()
    val traningData = inputData.map{ case (f, i) => (f.values, i)}
    val result  = rf.run(traningData, labels, nTrees)  
        
    // build index for names
    val index = inputData.map({case (f,i) => (i, f.label)}).collectAsMap()
    val varImportance = result.variableImportance.toSeq.sortBy(-_._2).take(limit).map({ case (i, importance) => (index(i), importance)})
    
    if (isEcho && outputFile!=null) {
      echo("Variable importance preview")
      varImportance.take(math.min(limit, defaultPreviewSize)).foreach({case(label, importance) => echo(s"${label}: ${importance}")})
    }
    
    LoanUtils.withCloseable(if (outputFile != null ) CSVWriter.open(outputFile) else CSVWriter.open(System.out)) { writer =>
      writer.writeRow(List("variable","importance"))
      writer.writeAll(varImportance.map(t => t.productIterator.toSeq))
    }
    
    
  }
}

object ImportanceCmd  {
  def main(args:Array[String]) {
    AppRunner.mains[ImportanceCmd](args)
  }
}
