package au.csiro.variantspark.hail

import is.hail.variant.VariantDataset
import au.csiro.variantspark.hail.methods.RfImportanceAnalysis


class VSHailFunctions(val vds:VariantDataset) extends AnyVal {
  
  //private def requireSplit(methodName: String) {
  //  if (!vds.wasSplit)
  //    fatal(s"method `$methodName' requires a split dataset. Use `split_multi' or `filter_multi' first.")
  //}
  
  def importanceAnalysis(y: String, root: String = "va.rf", nTopVariables:Int = 1000):VariantDataset = {
    //requireSplit("logistic regression")
    RfImportanceAnalysis(vds, y, root, nTopVariables)
  }
}

