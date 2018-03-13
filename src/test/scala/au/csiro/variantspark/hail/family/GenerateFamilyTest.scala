package au.csiro.variantspark.hail.family

import au.csiro.variantspark.test.SparkTest
import org.junit.Test
import org.junit.Assert._

import is.hail.HailContext
import is.hail.variant._
import is.hail.utils._
import is.hail.expr._
import au.csiro.variantspark.hail._
import au.csiro.variantspark.genomics.family.PedigreeTree
import au.csiro.variantspark.genomics.family.FamilySpec
import au.csiro.variantspark.genomics.impl.SimpleMeiosisSpecFactory
import au.csiro.variantspark.genomics.ReferenceContigSet
import au.csiro.variantspark.genomics.reprod.GameteSpecFactory

class GenerateFamilyTest extends SparkTest {
  
  @Test
  def testGenerateFamilyFromPed() {
    val hc = HailContext(sc)
    val gds = hc.importVCFGenericEx("data/hipsterIndex/hipster.vcf.bgz")
    println(" globalSignature: "  + gds.globalSignature)
    println(gds.count())
    
    val tree = PedigreeTree.loadPed("data/relatedness/g1k_ceu_family_15_2.ped")
    val meiosisFactory  = new SimpleMeiosisSpecFactory(ReferenceContigSet.b37)
    val gameteFactory = GameteSpecFactory(meiosisFactory)
    val familySpec = FamilySpec.apply(tree, gameteFactory)

    val familyGds = GenerateFamily(familySpec)(gds)
    familyGds.exportVCFEx("target/g1k_ceu_family_15_2.vcf")
  }
}