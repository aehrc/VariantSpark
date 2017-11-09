package au.csiro.variantspark.hail

import is.hail.variant.VariantDataset
import au.csiro.variantspark.hail.methods.RfImportanceAnalysis


class VSHailFunctions(val vds:VariantDataset) extends AnyVal {
  
  //private def requireSplit(methodName: String) {
  //  if (!vds.wasSplit)
  //    fatal(s"method `$methodName' requires a split dataset. Use `split_multi' or `filter_multi' first.")
  //}
  
  def importanceAnalysis(y: String, nTrees:Int=1000, 
        mtryFraction:Option[Double] = None, oob:Boolean = true, seed: Option[Long] = None, batchSize:Int = 100):RfImportanceAnalysis = {
    //requireSplit("logistic regression")
    RfImportanceAnalysis(vds, y, nTrees, mtryFraction, oob,  seed, batchSize)
  }
}

