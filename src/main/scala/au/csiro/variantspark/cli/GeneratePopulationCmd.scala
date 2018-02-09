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
import au.csiro.pbdava.ssparkle.common.utils.LoanUtils
import java.io.FileWriter

/**
 * Generates specification of a synthetic population based on 
 * provided pedigree and genomic map for recombination
 */
class GeneratePopulationCmd extends ArgsApp with Logging with TestArgs with Echoable {

  @Option(name="-of", required=true, usage="Path to output speficification file", aliases=Array("--output-file"))
  val outputFile:String = null

  @Option(name="-pf", required=true, usage="Path to pedigree file", aliases=Array("--ped-file"))
  val pedFile:String = null

  @Option(name="-bf", required=true, usage="Path bed file with recombination map", aliases=Array("--bed-file"))
  val bedFile:String = null
  
  @Option(name="-sr", required=false, usage="Random seed to use (def=<random>)", aliases=Array("--seed"))
  val randomSeed: Long = defRng.nextLong

  
  @Override
  def testArgs = Array(
      "-of", "target/g1k_ceu_family_15_2.spec.json",
      "-pf", "data/relatedness/g1k_ceu_family_15_2.ped", 
      "-bf", "data/relatedness/genetic_map_GRCh37_1Mb.bed.gz",
      "-sr", "13"
      )      
  
  @Override
  def run():Unit = {
    logInfo("Running with params: " + ToStringBuilder.reflectionToString(this))
    echo(s"Loading pedigree from: ${pedFile}")     
    val tree = PedigreeTree.loadPed(pedFile)    
    echo(s"Loading genetic map from: ${bedFile}") 
    val gameteFactory = HapMapGameteSpecFactory.fromBedFile(bedFile, randomSeed)
    //val gameteFactory  = new SimpleGameteSpecFactory(ReferenceContigSet.b37)
    val familySpec = FamilySpec.apply(tree, gameteFactory)
    echo(s"Writing population spec to: ${outputFile}")
    LoanUtils.withCloseable(new FileWriter(outputFile)) { w =>
      familySpec.toJson(w)
    }
  }
}

object GeneratePopulationCmd  {
  def main(args:Array[String]) {
    AppRunner.mains[GeneratePopulationCmd](args)
  }
}
