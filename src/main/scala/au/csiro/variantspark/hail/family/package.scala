package au.csiro.variantspark.hail

import is.hail.variant.Variant
import au.csiro.variantspark.pedigree.GenomicPos

package object family {
  implicit def fromVariantToGenomicPos(v: Variant): GenomicPos = GenomicPos(v.contig, v.start)
}