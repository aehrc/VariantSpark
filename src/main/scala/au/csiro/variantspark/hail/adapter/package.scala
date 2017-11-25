package au.csiro.variantspark.hail

import is.hail.HailContext


/** Adapters for using variant-spark API with Hail. The general way of using it is:
  * 	val hc = HailContext() 
  *   val vds = ...
  *   ...
  *   import au.csiro.variantspark.hail._
  *   implicit val vsc =  HailContextAdapter(hc)
  *   val featureSource = HailFeatureSource(vds)
  *   val labelSource =  HailLabelSource(vds, "sa.pheno.label")
  *   val ia = ImportanceAnalysis(featureSource, labelSource) 
  *   ...
  */
package object adapter {
  implicit def  hailContextToSqlContextHolder(hc:HailContext) = new HailContextAdapter(hc)
}