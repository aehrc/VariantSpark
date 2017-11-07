package au.csiro.variantspark.hail.adapter

import au.csiro.variantspark.input.LabelSource
import is.hail.variant.VariantDataset
import org.apache.spark.sql.Row
import is.hail.stats.RegressionUtils

/** Implements the variant-spark HailLabelSource on a Hail VariantDataset. 
  * Extract the labels from sample annotations.
  * 
  * @param vds a hail VariantDataset
  * @param yExpr a hail expression to extract the labels from sample annotations. 
  * 			The expression must to boolean or numeric values equal to 0 or 1.
  */
class HailLabelSource(val vds: VariantDataset, val yExpr:String) extends LabelSource {
  
  def getLabels(labels:Seq[String]):Array[Int] = {    
    val (y,_,_) = RegressionUtils.getPhenoCovCompleteSamples(vds, yExpr, Array())
    if (!y.forall(yi => yi == 0d || yi == 1d))
      throw new RuntimeException("Phenotype must be Boolean or numeric with all present values equal to 0 or 1")
    y.valuesIterator.map(_.toInt).toArray
  }
}

object HailLabelSource {
  def apply(vds: VariantDataset, yExpr:String) = new HailLabelSource(vds,yExpr)
}