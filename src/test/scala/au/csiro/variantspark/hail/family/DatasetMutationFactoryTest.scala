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
import au.csiro.variantspark.genomics.reprod.Mutation
import au.csiro.variantspark.genomics.GenomicCoord

class DatasetMutationFactoryTest extends SparkTest {
  
  @Test
  def testGenerateValidMutations {
    
    val variants = sc.parallelize(Seq(
        Variant.apply("1", 1, "C", "G"),
        Variant.apply("1", 2, "C", Array("G", "A")),
        Variant.apply("1", 3, "C", Array("G", "A", "T")), // all taken
        Variant.apply("1", 4, "C", "CTTT"),  // not snp
        Variant.apply("2", 1, "A", "C")
    ))
    
    val mutations = DatasetMutationFactory.toMutations(variants).collect().toSet
    assertEquals(Set(
          Mutation(GenomicCoord("1", 1), "C", "T"),
          Mutation(GenomicCoord("1", 1), "C", "A"),
          Mutation(GenomicCoord("1", 2), "C", "T"),
          Mutation(GenomicCoord("2", 1), "A", "T"),          
          Mutation(GenomicCoord("2", 1), "A", "G")        
        ), mutations)
  }
  
}