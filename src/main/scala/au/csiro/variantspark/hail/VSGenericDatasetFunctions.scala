package au.csiro.variantspark.hail

import is.hail.variant.GenericDataset
import au.csiro.variantspark.hail.variant.io.ExportVCFEx

class VSGenericDatasetFunctions(val gds: GenericDataset) extends AnyVal {
  
  def exportVCFEx(outputPath: String)  {
    ExportVCFEx(gds, outputPath)
  }
  
}