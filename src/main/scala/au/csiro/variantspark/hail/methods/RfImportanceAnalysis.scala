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
import is.hail.expr.TVariant
import org.apache.spark.sql.Row
import au.csiro.variantspark.algo.RandomForestParams


class RfImportanceAnalysis(val hc: HailContext, importanceAnalysis: ImportanceAnalysis) {

  val oobError: Double  = importanceAnalysis.rfModel.oobError
  
  def variantImportance(nLimit:Int = 1000): KeyTable  = {    
    val importantVariants = importanceAnalysis.importantVariables(nLimit)
      .map { case (label, importance) => Row(RfImportanceAnalysis.labelToVariant(label), importance) }
    KeyTable(hc, hc.sc.parallelize(importantVariants), RfImportanceAnalysis.impSignature, Array("variant"))
  }
}

object RfImportanceAnalysis {
  
  val defaultRFParams = RandomForestParams()
 
  val impSignature = TStruct(("variant", TVariant), ("importance",TDouble))
   
  def labelToVariant(label:String):Variant = {
    val contiPositionRefAlt = label.split(":")
    Variant(contiPositionRefAlt(0), contiPositionRefAlt(1).toInt, 
        contiPositionRefAlt(2), contiPositionRefAlt(3))
  }
  
  val schema: Type = TStruct(
    ("giniImportance", TDouble)
  )
  
  def apply(vds: VariantDataset, yExpr:String, nTrees:Int, mtryFraction:Option[Double], oob:Boolean, 
        seed: Option[Long] , batchSize:Int):RfImportanceAnalysis = {
    require(vds.wasSplit)
    val featureSource = HailFeatureSource(vds)
    val labelSource = HailLabelSource(vds, yExpr)
    implicit val vsContext = HailContextAdapter(vds.hc)
    new RfImportanceAnalysis(vds.hc, ImportanceAnalysis.fromParams(featureSource, labelSource, 
        rfParams = RandomForestParams(
              nTryFraction = mtryFraction.getOrElse(defaultRFParams.nTryFraction), 
              seed =  seed.getOrElse(defaultRFParams.seed), 
              oob = oob
          ),
          nTrees = nTrees,
          batchSize = batchSize      
        ))
  }
}