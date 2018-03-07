package au.csiro.variantspark.cli

import java.io.File

import scala.Range
import scala.{Option => SOption}
import org.apache.commons.lang3.builder.ToStringBuilder
import au.csiro.pbdava.ssparkle.common.utils.Logging
import org.kohsuke.args4j.Option

import au.csiro.pbdava.ssparkle.common.arg4j.AppRunner
import au.csiro.pbdava.ssparkle.common.arg4j.TestArgs
import au.csiro.sparkle.common.args4j.ArgsApp
import au.csiro.pbdava.ssparkle.spark.SparkApp
import au.csiro.variantspark.cmd.EchoUtils._
import au.csiro.variantspark.cmd.Echoable
import au.csiro.variantspark.utils.defRng

import au.csiro.variantspark.pedigree.ReferenceContigSet
import is.hail.HailContext
import au.csiro.variantspark.hail.family.GenerateFamily
import au.csiro.variantspark.pedigree.FamilySpec
import au.csiro.variantspark.pedigree.impl.SimpleMeiosisSpecFactory
import au.csiro.variantspark.pedigree.PedigreeTree
import au.csiro.variantspark.hail._
import au.csiro.variantspark.pedigree.impl.HapMapMeiosisSpecFactory
import au.csiro.pbdava.ssparkle.common.utils.LoanUtils
import java.io.FileWriter
import au.csiro.variantspark.pedigree.GameteSpecFactory
import au.csiro.variantspark.pedigree.MutationSetFactory
import au.csiro.variantspark.hail.family.DatasetMutationFactory
import au.csiro.variantspark.pedigree.Defaults

/**
 * Generates specification of a synthetic population based on 
 * provided pedigree and genomic map for recombination
 */
class GenerateFamilyCmd extends ArgsApp  with SparkApp with Logging with TestArgs with Echoable {

  override def createConf = super.createConf
    .set("spark.sql.files.openCostInBytes", "53687091200") // 50GB : min for hail 
    .set("spark.sql.files.maxPartitionBytes", "53687091200") // 50GB : min for hail 

  
  @Option(name="-of", required=true, usage="Path to output speficification file", aliases=Array("--output-file"))
  val outputFile:String = null

  @Option(name="-pf", required=true, usage="Path to pedigree file", aliases=Array("--ped-file"))
  val pedFile:String = null

  @Option(name="-bf", required=true, usage="Path bed file with recombination map", aliases=Array("--bed-file"))
  val bedFile:String = null
  
  @Option(name="-vf", required=false, usage="Path to input vcf file to draw mutations from", 
        aliases=Array("--variant-file"))
  val variantFile:String = null
  
  
  @Option(name="-sr", required=false, usage="Random seed to use (def=<random>)", aliases=Array("--seed"))
  val randomSeed: Long = defRng.nextLong

  @Option(name="-mp", required=false, usage="Min partition to use for input dataset(default=spark.default.pararellism)"
      , aliases=Array("--min-partitions"))
  val minPartitions: Int = -1
  
  @Override
  def testArgs = Array(
      "-of", "target/g1k_ceu_family_15_2.spec.json",
      "-pf", "data/relatedness/g1k_ceu_family_15_2.ped", 
      "-bf", "data/relatedness/genetic_map_GRCh37_1Mb.bed.gz",
      "-vf", "data/hipsterIndex/hipster.vcf.bgz",
      "-sr", "13"
      )      
 
      
  def loadMutationsFactory(inputFile:String):DatasetMutationFactory = {  
    val actualMinPartitions = if (minPartitions > 0) minPartitions else  sc.defaultParallelism 
    echo(s"Loadig mutations from vcf from ${inputFile} with ${actualMinPartitions} partitions")
    val hc = HailContext(sc)
    val variantsRDD = hc.importVCFSnps(inputFile.split(","), nPartitions = Some(actualMinPartitions))
    new DatasetMutationFactory(variantsRDD, mutationRate = Defaults.humanMutationRate, 
        contigSet = ReferenceContigSet.b37, randomSeed)
  }
      
  @Override
  def run():Unit = {
    logInfo("Running with params: " + ToStringBuilder.reflectionToString(this))
    echo(s"Loading pedigree from: ${pedFile}")     
    val tree = PedigreeTree.loadPed(pedFile)    
    echo(s"Loading genetic map from: ${bedFile}") 
    val meiosisFactory = HapMapMeiosisSpecFactory.fromBedFile(bedFile, randomSeed)
    
    //DatasetMutationFactory
    val mutationsFactory =  SOption(variantFile).map(loadMutationsFactory _) 
    val gameteFactory = GameteSpecFactory(meiosisFactory, mutationsFactory)
    val familySpec = FamilySpec.apply(tree, gameteFactory)
    echo(s"Writing population spec to: ${outputFile}")
    LoanUtils.withCloseable(new FileWriter(outputFile)) { w =>
      familySpec.toJson(w)
    }
  }
}

object GenerateFamilyCmd  {
  def main(args:Array[String]) {
    AppRunner.mains[GeneratePopulationCmd](args)
  }
}
