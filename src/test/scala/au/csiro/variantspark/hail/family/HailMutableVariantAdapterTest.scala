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
