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

class GenerateFamilyIntegrationTest extends SparkTest {
  
  @Test
  def testGenerateFamilyFromPed() {
    val hc = HailContext(sc)
    val contigSet = ReferenceContigSet.b37.onlyAutosomes()    
    // load meiosis spec factory
    val recombinationMap = RecombinationMap.fromBedFile("data/relatedness/genetic_map_GRCh37_1Mb.bed.gz")
        .filter(contigSet)
    val meiosisFactory = HapMapMeiosisSpecFactory.apply(recombinationMap, 13L)
    // load mutation factory
    val variantsRDD = hc.importVCFSnps(List("data/relatedness/g1k_sample.vcf.bgz"))
    val mutationFactory = DatasetMutationFactory(variantsRDD, mutationRate = Defaults.humanMutationRate,
          contigSet = contigSet, seed = 13L)   
    
    val gameteFactory = GameteSpecFactory(meiosisFactory, Some(mutationFactory))
    val tree = PedigreeTree.loadPed("data/relatedness/g1k_ceu_family_15_2.ped")
    val familySpec = FamilySpec.apply(tree, gameteFactory)

    // all members are in the result
    assertEquals(tree.orderedTrioIds.toSet, familySpec.memberIds.toSet)
    // check all founders are here
    assertEquals(tree.trios.filter(_.isFounder).map(_.id).toSet, familySpec.founderIds.toSet)
    
    // validate correct number of mutations
    val totalMutations = familySpec.offsprings.map(o =>
        o.offspring.fatherGamete.mutationSet.mutations.size + o.offspring.motherGamete.mutationSet.mutations.size).sum
    val avgMutationsPerGenration = totalMutations.toDouble/familySpec.offsprings.size
    val actualMutationRate = avgMutationsPerGenration/(contigSet.totalLenght*2.0) // twice to account for pairs of chromosomee
    assertEquals(Defaults.humanMutationRate, actualMutationRate, 1e-8)    
  }
}