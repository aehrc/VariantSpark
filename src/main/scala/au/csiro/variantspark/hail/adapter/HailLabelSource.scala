package au.csiro.variantspark.hail.adapter

import au.csiro.variantspark.input.LabelSource
import is.hail.variant.VariantDataset
import org.apache.spark.sql.Row

class HailLabelSource(val vds: VariantDataset, val saPath:List[String]) extends LabelSource {
  
  def getLabels(labels:Seq[String]):Array[Int] = {
    
    val saQuery =  vds.saSignature.query(saPath)
    val sampleAnnotationsAsMap = vds.sampleIdsAndAnnotations.map(t => (t._1.asInstanceOf[String], t._2.asInstanceOf[Row])).toMap
    //TODO: Better conversion to Int
    labels.map(l => saQuery(sampleAnnotationsAsMap(l)).toString.toInt).toArray
  }
}