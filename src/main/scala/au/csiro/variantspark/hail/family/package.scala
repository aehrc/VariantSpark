package au.csiro.variantspark.hail

import is.hail.variant.Variant
import au.csiro.variantspark.pedigree.GenomicPos
import au.csiro.variantspark.hail.variant.phased.BiCall
import au.csiro.variantspark.pedigree.GenotypeSpec
import is.hail.variant.GTPair

package object family {
  implicit def fromVariantToGenomicPos(v: Variant): GenomicPos = GenomicPos(v.contig, v.start)
  implicit def fromBiCallToGenotypeSpec(bc: BiCall): GenotypeSpec = GenotypeSpec(bc(0), bc(1))
  implicit def fromGenotypeSpecToBiCall(gs: GenotypeSpec): BiCall =  BiCall(GTPair.apply(gs(0), gs(1)))

}