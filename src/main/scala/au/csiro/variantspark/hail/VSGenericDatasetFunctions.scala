package au.csiro.variantspark.hail

import is.hail.variant.GenericDataset
import au.csiro.variantspark.hail.variant.io.ExportVCFEx
import au.csiro.variantspark.pedigree.OffspringSpec
import au.csiro.variantspark.hail.family.GenerateOffspring

class VSGenericDatasetFunctions(val gds: GenericDataset) extends AnyVal {
  
  def exportVCFEx(outputPath: String)  {
    ExportVCFEx(gds, outputPath)
  }
  
  def generateOffspring(offspringSpec: OffspringSpec):GenericDataset = {
    new GenerateOffspring(offspringSpec).apply(gds)
  }
  
  
}