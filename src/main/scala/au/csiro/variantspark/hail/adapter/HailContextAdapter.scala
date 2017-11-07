package au.csiro.variantspark.hail.adapter

import au.csiro.variantspark.api.SqlContextHolder
import is.hail.HailContext


/** An adapter for HailContext to use with variant-spark API
 	* @param hc HailContext to adapt 
 	*/
class HailContextAdapter(val hc:HailContext) extends SqlContextHolder {
  def sqlContext = hc.sqlContext
}

object HailContextAdapter {
  def apply(hc:HailContext) = new HailContextAdapter(hc)
}