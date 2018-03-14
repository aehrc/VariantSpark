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
import au.csiro.variantspark.genomics.impl.HapMapMeiosisSpecFactory
import au.csiro.variantspark.genomics.reprod.RecombinationMap
import au.csiro.variantspark.genomics.family.Founder
import au.csiro.variantspark.genomics.family.Offspring
import au.csiro.variantspark.genomics._
import au.csiro.pbdava.ssparkle.common.utils.LoanUtils
import scala.io.Source

class GeneratePopulationIntegrationTest extends SparkTest {
  
  @Test
  def testGeneratePopulationFromSpec() {
    val hc = HailContext(sc)
    val familySpec = LoanUtils.withSource(Source
      .fromFile("data/relatedness/g1k_ceu_family_15_2.spec.json"))(s => FamilySpec.fromJson(s))

    val gds = hc.importVCFGenericEx("data/relatedness/g1k_sample.vcf.bgz", nPartitions = Some(2))  
    val familyGds = GenerateFamily(familySpec)(gds)
    assertEquals(familySpec.memberIds.toSet, familyGds.sampleIds.toSet)    
 }
}