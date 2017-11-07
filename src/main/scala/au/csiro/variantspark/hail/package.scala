package au.csiro.variantspark

import is.hail.variant.VariantDataset

package object hail {
  implicit def toVSHailFunctions(vds:VariantDataset) = new VSHailFunctions(vds)
}