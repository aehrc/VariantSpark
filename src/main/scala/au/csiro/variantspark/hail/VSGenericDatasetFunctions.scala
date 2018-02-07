package au.csiro.variantspark.hail

import is.hail.variant.GenericDataset
import au.csiro.variantspark.hail.variant.io.ExportVCFEx
import au.csiro.variantspark.pedigree.OffspringSpec
import au.csiro.variantspark.pedigree.FamilySpec
import au.csiro.variantspark.hail.family.GenerateFamily

class VSGenericDatasetFunctions(val gds: GenericDataset) extends AnyVal {
  
  def exportVCFEx(outputPath: String)  {
    ExportVCFEx(gds, outputPath)
  }
  
  def generateFamily(familySpec: FamilySpec): GenericDataset = {
    GenerateFamily(familySpec)(gds)
  }
  
}