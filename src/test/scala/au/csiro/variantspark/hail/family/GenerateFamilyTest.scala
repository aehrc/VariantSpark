package au.csiro.variantspark.hail.family

import au.csiro.variantspark.test.SparkTest
import org.junit.Test
import org.junit.Assert._

import is.hail.HailContext
import is.hail.variant._
import is.hail.utils._
import is.hail.expr._
import au.csiro.variantspark.hail._
import au.csiro.variantspark.pedigree.PedigreeTree
import au.csiro.variantspark.pedigree.FamilySpec
import au.csiro.variantspark.pedigree.SimpleHomozigotSpecFactory
import au.csiro.variantspark.pedigree.ReferenceContigSet

class GenerateFamilyTest extends SparkTest {
  
  @Test
  def testGenerateFamilyFromPed() {
    val hc = HailContext(sc)
    val gds = hc.importVCFGenericEx("data/hipsterIndex/hipster.vcf.bgz")
    println(" globalSignature: "  + gds.globalSignature)
    println(gds.count())
    
    val tree = PedigreeTree.loadPed("data/relatedness/g1k_ceu_family_15_2.ped")
    val gameteFactory  = new SimpleHomozigotSpecFactory(ReferenceContigSet.b37)
    val familySpec = FamilySpec.apply(tree, gameteFactory)

    val familyGds = GenerateFamily(familySpec)(gds)
    familyGds.exportVCFEx("target/g1k_ceu_family_15_2.vcf")
  }
}