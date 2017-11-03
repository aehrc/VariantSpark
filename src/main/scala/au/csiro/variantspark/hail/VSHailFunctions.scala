package au.csiro.variantspark.hail

import is.hail.variant.VariantDataset


class VSHailFunctions(val vds:VariantDataset) extends AnyVal {
  def xxx() = vds.rdd.count()
}

object VSHailFunctions {
  implicit def toVSHailFunctions(vds:VariantDataset) = new VSHailFunctions(vds)
}