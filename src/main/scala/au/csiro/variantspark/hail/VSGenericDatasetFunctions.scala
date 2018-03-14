package au.csiro.variantspark.hail

import is.hail.variant.GenericDataset
import au.csiro.variantspark.hail.variant.io.ExportVCFEx
import au.csiro.variantspark.genomics.family.FamilySpec
import au.csiro.variantspark.hail.family.GenerateFamily

class VSGenericDatasetFunctions(val gds: GenericDataset) extends AnyVal {
  
  def exportVCFEx(outputPath: String, parallel:Boolean = false)  {
    ExportVCFEx(gds, outputPath, parallel = parallel)
  }
  
  def generateFamily(familySpec: FamilySpec): GenericDataset = {
    GenerateFamily(familySpec)(gds)
  }
  
}