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
import au.csiro.variantspark.pedigree.impl.SimpleMeiosisSpecFactory
import au.csiro.variantspark.pedigree.ReferenceContigSet
import au.csiro.variantspark.pedigree.GameteSpecFactory


class HailMutableVariantAdapterTest {
  
  @Test
  def testIndexOfExistingAlt() {
    val variant = Variant("1", 12, "C", "G")
    val va = new HailMutableVariantAdapter(variant)
    assertEquals(0, va.getOrElseUpdate("C"))
    assertEquals(1, va.getOrElseUpdate("G"))
    assertEquals(variant, va.toVariant)
  }

  @Test
  def testIndexOfNewAlts() {
    val variant = Variant("1", 12, "C", "G")
    val va = new HailMutableVariantAdapter(variant)
    assertEquals(0, va.getOrElseUpdate("C"))
    assertEquals(1, va.getOrElseUpdate("G"))
    assertEquals(2, va.getOrElseUpdate("T"))
    assertEquals(3, va.getOrElseUpdate("A"))
    // check if the second time we get the same values
    assertEquals(0, va.getOrElseUpdate("C"))
    assertEquals(1, va.getOrElseUpdate("G"))
    assertEquals(2, va.getOrElseUpdate("T"))
    assertEquals(3, va.getOrElseUpdate("A"))
    assertEquals(Variant("1", 12, "C", Array("G", "T", "A")), va.toVariant)
  }
 
}

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