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

import au.csiro.variantspark.genomics.ReferenceContigSet
import is.hail.HailContext
import au.csiro.variantspark.hail.family.GenerateFamily
import au.csiro.variantspark.genomics.family.FamilySpec
import au.csiro.variantspark.genomics.impl.SimpleMeiosisSpecFactory
import au.csiro.variantspark.genomics.family.PedigreeTree
import au.csiro.variantspark.hail._
import au.csiro.variantspark.genomics.impl.HapMapMeiosisSpecFactory
import au.csiro.pbdava.ssparkle.common.utils.LoanUtils
import java.io.FileWriter
import au.csiro.variantspark.genomics.reprod.GameteSpecFactory
import au.csiro.variantspark.genomics.reprod.MutationSetFactory
import au.csiro.variantspark.hail.family.DatasetMutationFactory
import au.csiro.variantspark.genomics.Defaults
import au.csiro.variantspark.cli.args.HailArgs
import au.csiro.variantspark.genomics.ContigSet
import au.csiro.variantspark.genomics.reprod.RecombinationMap

/**
 * Generates specification of a synthetic population based on 
 * provided pedigree and recombination map. 
 * The recombination map defines the genome build to use (at least to the extend that it defined the contigs 
 * and their lengths)
 * Optionally it also generates mutations based on provided VCF file as a source of mutation variants.
 * The file needs to use the build (contig spec as the genomic map)
 * Currently only autosomes are supported.
 */
class GenerateFamilyCmd extends ArgsApp  with SparkApp with HailArgs with Logging with TestArgs with Echoable {
  
  @Option(name="-of", required=true, usage="Path to output population spec file", aliases=Array("--output-file"))
  val outputFile:String = null

  @Option(name="-pf", required=true, usage="Path to pedigree file", aliases=Array("--ped-file"))
  val pedFile:String = null

  @Option(name="-bf", required=true, usage="Path bed file with recombination map", aliases=Array("--bed-file"))
  val bedFile:String = null
  
  @Option(name="-vf", required=false, usage="Optional path to a vcf file to draw mutations from (default=None)", 
        aliases=Array("--variant-file"))
  val variantFile:String = null

  @Option(name="-mr", required=false, usage="Mutation rate in [bps/generation] (default = 1.1e-8)", 
        aliases=Array("--mutation-rate"))
  val mutationRate:Double = 1.1e-8

  @Option(name="-sr", required=false, usage="Random seed to use (def=<random>)", aliases=Array("--seed"))
  val randomSeed: Long = defRng.nextLong

  @Override
  def testArgs = Array(
      "-of", "target/g1k_ceu_family_15_2.spec.json",
      "-pf", "data/relatedness/g1k_ceu_family_15_2.ped", 
      "-bf", "data/relatedness/genetic_map_GRCh37_1Mb.bed.gz",
      "-vf", "data/relatedness/g1k_sample.vcf.bgz",
      "-sr", "13", 
      "-mp", "2"
      )      
 
          
  def loadMutationsFactory(contigSet:ContigSet)(inputFile:String):DatasetMutationFactory = {  
    echo(s"Loadig mutations from vcf:  ${inputFile} with ${actualMinPartitions} partitions")
    val variantsRDD = hc.importVCFSnps(inputFile.split(","), nPartitions = Some(actualMinPartitions))
    DatasetMutationFactory(variantsRDD, mutationRate = mutationRate, 
        contigSet = contigSet, randomSeed)
  }
      
  @Override
  def run():Unit = {
    logInfo("Running with params: " + ToStringBuilder.reflectionToString(this))
    echo(s"Loading pedigree from: ${pedFile}")
    //TODO: load from HDFS
    val tree = PedigreeTree.loadPed(pedFile)    
    val contigSet = ReferenceContigSet.b37.onlyAutosomes()
    echo(s"Using contig set: ${contigSet}")
    //TODO: load from HDFS
    echo(s"Loading genetic map from: ${bedFile}")     
    val recombinationMap = RecombinationMap.fromBedFile(bedFile).filter(contigSet)
    val meiosisFactory = HapMapMeiosisSpecFactory.apply(recombinationMap, randomSeed)
    val mutationsFactory = SOption(variantFile).map(loadMutationsFactory(contigSet)) 
    if (mutationsFactory.isEmpty) {
      echo("Mutation source not provided - skpping mutations")
    }
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
    AppRunner.mains[GenerateFamilyCmd](args)
  }
}
