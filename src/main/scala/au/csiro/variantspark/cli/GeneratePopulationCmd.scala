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

import au.csiro.variantspark.genomics.ReferenceContigSet
import is.hail.HailContext
import au.csiro.variantspark.hail.family.GenerateFamily
import au.csiro.variantspark.genomics.family.FamilySpec
import au.csiro.variantspark.genomics.impl.SimpleMeiosisSpecFactory
import au.csiro.variantspark.genomics.family.PedigreeTree
import au.csiro.variantspark.hail._
import au.csiro.variantspark.genomics.impl.HapMapMeiosisSpecFactory
import scala.io.Source
import au.csiro.pbdava.ssparkle.common.utils.LoanUtils
import au.csiro.variantspark.cli.args.HailArgs
import au.csiro.variantspark.cli.args.HailArgs

/**
 * A command to generate an synthetic population based on population specification.
 * It takes a source VCF files and generates a new VCF files with founders and offspring 
 * as described in the population specification file.
 */

class GeneratePopulationCmd extends ArgsApp with SparkApp with HailArgs with Logging with TestArgs with Echoable {

  @Option(name="-if", required=true, usage="Path to input vcf file", aliases=Array("--input-file"))
  val inputFile:String = null

  @Option(name="-of", required=true, usage="Path to output vcf file", aliases=Array("--output-file"))
  val outputFile:String = null
  
  @Option(name="-sf", required=true, usage="Path the population spec file", aliases=Array("--spec-file"))
  val specFile:String = null
  
  @Option(name="-sp", required=false, usage="Save bgz in parallel"
      , aliases=Array("--save-parallel"))
  val saveParallel: Boolean = false

  @Override
  def testArgs = Array("-if", "data/relatedness/g1k_sample.vcf.bgz", 
      "-of", "target/g1k_ceu_family_15_2.vcf.bgz",
      "-sf", "target/g1k_ceu_family_15_2.spec.json",
      "-mp", "4"
      )      
  
  @Override
  def run():Unit = {
    logInfo("Running with params: " + ToStringBuilder.reflectionToString(this))
    echo(s"Loadig vcf from ${inputFile} with ${actualMinPartitions} partitions")
    val gds = hc.importVCFsGenericEx(inputFile.split(","), nPartitions = Some(actualMinPartitions))
    println(s"Loading family spec from ${specFile}")
    //TODO: add loading from HDFS
    val familySpec = LoanUtils.withSource(Source.fromFile(specFile))(s => FamilySpec.fromJson(s))
    val familySpecSummary = familySpec.summary
    echo(s"Population of ${familySpecSummary.total} members => founders: ${familySpecSummary.noFounders}, offspring: ${familySpecSummary.noOffspring}")
    
    // check for missing family members
    val missingFamilyMemberIds = familySpec.founderIds.toSet.diff(gds.sampleIds.toSet)
    if (!missingFamilyMemberIds.isEmpty) {
      echo(s"${missingFamilyMemberIds} missing in the vcf file. Exiting...")
      System.exit(1)
    }
    val familyGds = GenerateFamily(familySpec)(gds)
    echo(s"Saving population vcf to: ${outputFile}")
    familyGds.exportVCFEx(outputFile, saveParallel)
  }
}

object GeneratePoplulationCmd  {
  def main(args:Array[String]) {
    AppRunner.mains[GeneratePopulationCmd](args)
  }
}
