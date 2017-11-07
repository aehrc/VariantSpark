package au.csiro.variantspark.hail.methods

import is.hail.variant.VariantDataset
import au.csiro.variantspark.hail.adapter.HailFeatureSource
import au.csiro.variantspark.hail.adapter.HailLabelSource
import au.csiro.variantspark.hail.adapter.HailContextAdapter
import au.csiro.variantspark.api.ImportanceAnalysis
import is.hail.expr.Parser
import is.hail.annotations.Annotation
import is.hail.expr.Type
import is.hail.expr.TStruct
import is.hail.expr.TDouble
import is.hail.variant._
import is.hail.utils._

object RfImportanceAnalysis {
  
  val schema: Type = TStruct(
    ("giniImportance", TDouble)
  )
  
  def apply(vds: VariantDataset, yExpr:String, root:String, nTopVariables:Int):VariantDataset = {

    require(vds.wasSplit)
  
    val featureSource = HailFeatureSource(vds)
    val labelSource = HailLabelSource(vds, yExpr)
    implicit val vsContext = HailContextAdapter(vds.hc)
    val ia = ImportanceAnalysis(featureSource, labelSource)  
      
    val br_importantVariables = vds.hc.sc.broadcast(ia.importantVariables(nTopVariables).toMap)
    
    val pathVA = Parser.parseAnnotationRoot(root, Annotation.VARIANT_HEAD)
    val (newVAS, inserter) = vds.insertVA(RfImportanceAnalysis.schema, pathVA)
    
    vds.copy(rdd = vds.rdd.mapPartitions( { it =>
      it.map { case (v, (va, gs)) =>
        
        val rfAnnotation = Annotation(br_importantVariables.value.getOrElse(
            HailFeatureSource.variantToFeatureName(v), 0.0))
        
        val newAnnotation = inserter(va, rfAnnotation)
        assert(newVAS.typeCheck(newAnnotation))
        (v, (newAnnotation, gs))
      }
    }, preservesPartitioning = true).asOrderedRDD).copy(vaSignature = newVAS)
  }
}