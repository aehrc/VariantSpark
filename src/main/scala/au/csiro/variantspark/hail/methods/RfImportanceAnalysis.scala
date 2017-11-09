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
import is.hail.keytable.KeyTable
import is.hail.HailContext
import is.hail.expr.TLocus
import org.apache.spark.sql.Row
import au.csiro.variantspark.algo.RandomForestParams


class RfImportanceAnalysis(val hc: HailContext, importanceAnalysis: ImportanceAnalysis) {

  val oobError: Double  = importanceAnalysis.rfModel.oobError
  
  def variantImportance(nLimit:Int = 1000): KeyTable  = {    
    val importantVariants = importanceAnalysis.importantVariables(nLimit)
      .map { case (label, importance) => Row(RfImportanceAnalysis.labelToLocus(label), importance) }
    KeyTable(hc, hc.sc.parallelize(importantVariants), RfImportanceAnalysis.impSignature, Array("locus"))
  }
}

object RfImportanceAnalysis {
  
  val impSignature = TStruct(("locus", TLocus), ("importance",TDouble))
  
  def labelToLocus(label:String):Locus = {
    val contigAndPosition = label.split("_")
    Locus(contigAndPosition(0),contigAndPosition(1).toInt)
  }
  
  val schema: Type = TStruct(
    ("giniImportance", TDouble)
  )
    
  def apply(vds: VariantDataset, yExpr:String, nTrees:Int, mtryFraction:Option[Double], 
        seed: Option[Long] , batchSize:Int):RfImportanceAnalysis = {
    require(vds.wasSplit)
    val featureSource = HailFeatureSource(vds)
    val labelSource = HailLabelSource(vds, yExpr)
    implicit val vsContext = HailContextAdapter(vds.hc)
    val defRFParams = RandomForestParams()
    new RfImportanceAnalysis(vds.hc, ImportanceAnalysis(featureSource, labelSource, 
        rfParams = RandomForestParams(
              nTryFraction = mtryFraction.getOrElse(defRFParams.nTryFraction), 
              seed =  seed.getOrElse(defRFParams.seed)
          ),
          nTrees = nTrees,
          rfBatchSize = batchSize      
        ))
  }
  
  def annotateImportance(vds: VariantDataset, yExpr:String, root:String, nTopVariables:Int):VariantDataset = {
    
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