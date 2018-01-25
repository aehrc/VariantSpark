package au.csiro.variantspark.hail.family

import is.hail.variant.VariantDataset
import is.hail.variant.Genotype
import au.csiro.variantspark.pedigree.OffspringSpec
import au.csiro.variantspark.pedigree.GenomicPos

class GenerateOffspring(val offspringSpec:OffspringSpec) {
  
  private def genotypeToHamming(gt:Genotype) {
    
  }
  
  def apply(vds:VariantDataset): VariantDataset =  {
    
    // later on we can probably add all in the form of annotations
    
    vds.rdd.map { 
      case (variant, (x, genotypes)) => 
      // so based on the variant position we need to determine which part of the genotype to include in the offspring
      // so esentially need to create the offspring genotype from the spec
      // I coiuld technicall operate on the allele indexes rather then actual variants
      // I first need to find both alleles of the parents
      val motherGenotype = ??? //diploid must be phased
      val fatherGenotype = ??? //diploid must be phased
      val position = GenomicPos(variant.contig, variant.start)
      val childGenotype = offspringSpec.genotypeAt(position, motherGenotype, fatherGenotype)
      // onece we have that we need to add it to the list of genotypes for this sample
      // look at join to see how
      None
    }
    
  
    
    return null
  }
  
}