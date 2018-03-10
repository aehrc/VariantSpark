package au.csiro.variantspark.genomics.family

import au.csiro.variantspark.genomics.reprod.MutableVariant
import au.csiro.variantspark.genomics._

case class TestMutableVariant(val contig:ContigID, val pos:Long,  
    val ref:BasesVariant) extends MutableVariant {
  
  def getOrElseUpdate(alt: BasesVariant): IndexedVariant = ???
}