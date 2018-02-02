package au.csiro.variantspark

import is.hail.HailContext
import is.hail.variant.VariantDataset
import is.hail.variant.GenericDataset

package object hail {
  implicit def toVSHailFunctions(vds:VariantDataset) = new VSHailFunctions(vds)
  implicit def toVSHailContextFunctions(hc: HailContext) = new VSHailContextFunctions(hc)
  implicit def toVSGenericDatasetFunctions(gds: GenericDataset) = new VSGenericDatasetFunctions(gds)
}