package au.csiro.variantspark.cli

import java.io.File

import scala.Range

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
import au.csiro.variantspark.pedigree.impl.SimpleGameteSpecFactory
import au.csiro.variantspark.pedigree.PedigreeTree
import au.csiro.variantspark.hail._
import au.csiro.variantspark.pedigree.impl.HapMapGameteSpecFactory
import scala.io.Source
import au.csiro.pbdava.ssparkle.common.utils.LoanUtils

class GenerateFamilyCmd extends ArgsApp with SparkApp with Logging with TestArgs with Echoable {

  override def createConf = super.createConf
      .set("spark.sql.files.openCostInBytes", "53687091200") // 50GB : min for hail 
      .set("spark.sql.files.maxPartitionBytes", "53687091200") // 50GB : min for hail 
  
  @Option(name="-if", required=true, usage="Path to input vcf file", aliases=Array("--input-file"))
  val inputFile:String = null

  @Option(name="-of", required=true, usage="Path to output vcf file", aliases=Array("--output-file"))
  val outputFile:String = null

//  @Option(name="-pf", required=true, usage="Path to pedigree file", aliases=Array("--ped-file"))
//  val pedFile:String = null

//  @Option(name="-bf", required=true, usage="Path bed file with recombination map", aliases=Array("--bed-file"))
//  val bedFile:String = null
  
//  @Option(name="-sr", required=false, usage="Random seed to use (def=<random>)", aliases=Array("--seed"))
//  val randomSeed: Long = defRng.nextLong

  
  @Option(name="-sf", required=true, usage="Path the population spec file", aliases=Array("--spec-file"))
  val specFile:String = null
  
  @Option(name="-mp", required=false, usage="Min partition to use for input dataset(default=spark.default.pararellism)"
      , aliases=Array("--min-partitions"))
  val minPartitions: Int = -1
  
  @Override
  def testArgs = Array("-if", "data/hipsterIndex/hipster.vcf.bgz", 
      "-of", "target/g1k_ceu_family_15_2.vcf.bgz",
//      "-pf", "data/relatedness/g1k_ceu_family_15_2.ped", 
//      "-bf", "data/relatedness/genetic_map_GRCh37_1Mb.bed.gz",
//      "-sr", "13",
      "-sf", "target/g1k_ceu_family_15_2.spec.json",
      "-mp", "4"
      )      
  
  @Override
  def run():Unit = {
    logInfo("Running with params: " + ToStringBuilder.reflectionToString(this))
    val hc = HailContext(sc)
    val actualMinPartitions = if (minPartitions > 0) minPartitions else  sc.defaultParallelism 
    echo(s"Loadig vcf from ${inputFile} with ${actualMinPartitions} partitions")
    val gds = hc.importVCFsGenericEx(inputFile.split(","), nPartitions = Some(actualMinPartitions))
//    echo(s"Loading pedigree from: ${pedFile}")     
//    val tree = PedigreeTree.loadPed(pedFile)
    
//    echo(s"Loading genetic map from: ${bedFile}") 
//    val gameteFactory = HapMapGameteSpecFactory.fromBedFile(bedFile, randomSeed)
//    val gameteFactory  = new SimpleGameteSpecFactory(ReferenceContigSet.b37)
//    val familySpec = FamilySpec.apply(tree, gameteFactory)
    println(s"Loading family spec from ${specFile}")
    val familySpec = LoanUtils.withSource(Source.fromFile(specFile))(s => FamilySpec.fromJson(s))
    echo("Using family spec")
    familySpec.members.foreach(println _)
    val familyGds = GenerateFamily(familySpec)(gds)
    echo(s"Saving family vcf to: ${outputFile}")
    familyGds.exportVCFEx(outputFile, true)
  }
}

object GenerateFamilyCmd  {
  def main(args:Array[String]) {
    AppRunner.mains[GenerateFamilyCmd](args)
  }
}
