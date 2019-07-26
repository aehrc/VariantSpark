package au.csiro.variantspark.hail.methods

import au.csiro.variantspark.hail.adapter.HailFeatureSource
import au.csiro.variantspark.hail.adapter.HailLabelSource
import au.csiro.variantspark.hail.adapter.HailContextAdapter
import au.csiro.variantspark.api.ImportanceAnalysis
import is.hail.expr.Parser
import is.hail.annotations.Annotation
import is.hail.variant._
import is.hail.utils._
import is.hail.HailContext
import org.apache.spark.sql.Row
import au.csiro.variantspark.algo.RandomForestParams
import is.hail.expr.ir.MatrixIR
import is.hail.stats.RegressionUtils
import is.hail.expr.ir.Interpret
import is.hail.expr.ir.Pretty
import is.hail.expr.ir.MatrixValue
import au.csiro.variantspark.data.Feature
import au.csiro.variantspark.data.StdFeature
import au.csiro.variantspark.algo.DefTreeRepresentationFactory
import au.csiro.variantspark.data.BoundedOrdinalVariable
import au.csiro.variantspark.algo.RandomForest
import au.csiro.variantspark.algo.RandomForestModel
import is.hail.expr.ir.TableValue
import is.hail.expr.types.virtual.TStruct
import is.hail.expr.types.virtual.TInt64
import is.hail.expr.types.virtual.TFloat64
import is.hail.expr.ir.TableIR
import is.hail.expr.ir.TableLiteral
import au.csiro.variantspark.algo.TreeFeature
import org.apache.spark.rdd.RDD
import is.hail.expr.types.virtual.TString
import scala.collection.mutable.WrappedArray
import is.hail.expr.types.virtual.TLocus
import is.hail.expr.types.virtual.TArray
import scala.collection.IndexedSeq

case class RFModel(mv:MatrixValue) { 
 
  val sig = TStruct(
      // The keys fields need to be coppied from input
      ("locus", TLocus(ReferenceGenome.GRCh37)),
      ("alleles", TArray.apply(TString())),
      ("importance", TFloat64()))

  private val keys: IndexedSeq[String] = Array("locus", "alleles")
  
  
  var rfModel:RandomForestModel = null
  var inputData:RDD[TreeFeature] = null
  
  
  def fitTrees(nTrees:Int = 500, batchSize:Int = 100)  {
 
    // SPIKE: Obtain values for labels (y) variable
    // These are currently obrained as doubles and converted to Int's needed by RandomForest
    // This is because getPhenosCovCompleteSamples only works on Double64 columns 
    // This may be optimized in the future
    val (yMat, cov, completeColIdx) = RegressionUtils.getPhenosCovCompleteSamples(mv, Array("y"), Array[String]())
    //TODO: We should somehow incorporate the completeColIdx here - that is I assume indexes of samples that have non empty label value
    //println(completeColIdx.toList)
    val labelVector = yMat(::, 0)
    if (!labelVector.forall(yi => yi == 0d || yi == 1d))
        fatal(s"For logistic regression,  label must be bool or numeric with all present values equal to 0 or 1")
    val labels = labelVector.map(_.toInt).toArray
    println(labels, labels.toList.take(10))
    
    // now we somehow need to get to row data
   
    //println(mv.rvd.typ)
    //RVDType{key:[[locus,alleles]],row:Struct{locus:Locus(GRCh37),alleles:Array[String],`the entries! [877f12a8827e18f61222c6c8c5fb04a8]`:Array[Struct{e:Int32}]}}
    
    val featuresRDD = mv.rvd.toRows.map(RFModel.rowToFeature)
    //mv.rvd.toRows.map(r => (r.getSeq[Any](1), r.get(1).getClass())).take(3).foreach(println _)
    //mv.rvd.toRows.map(r => (r.getStruct(2), r.get(2).getClass())).take(3).foreach(println _)
    //println(mv.rvd.typ)
    
    inputData = DefTreeRepresentationFactory.createRepresentation(featuresRDD.zipWithIndex()).cache() 
    val totalVariables = inputData.count()
    println(s"Loaded ${totalVariables} variables")
    inputData.take(4).foreach(println _)
    
    
    val rf = new RandomForest()
    rfModel = rf.batchTrainTyped(inputData, labels, nTrees, batchSize)
  }
  
  def oobError:Double = rfModel.oobError

  def variableImportance:TableIR = {    
    
    val brVarImp = inputData.sparkContext.broadcast(rfModel.variableImportance)    
    val mapRDD  = inputData.mapPartitions { it =>
      val varImp = brVarImp.value
      it.map(tf => RFModel.tfFeatureToImpRow(tf.label, varImp.getOrElse(tf.index, 0.0)))
    }
    TableLiteral(TableValue(sig, keys, mapRDD))
  }
}

object RFModel {
    
  def tfFeatureToImpRow(label:String, impValue:Double):Row =  {
    val elements = label.split("_")
    val alleles = elements.drop(2).map(_.asInstanceOf[Annotation]).toIndexedSeq
    Row(Locus(elements(0), elements(1).toInt), alleles, impValue) // , elements.drop(2))
  }
  
  def rowToFeature(r:Row):Feature = {
    val locus = r.getAs[Locus](0)
    val varName = (Seq(locus.contig, locus.position.toString) ++ r.getSeq[String](1)).mkString("_")
    val data = r.getSeq[Row](2).map(_.getInt(0)).toArray
    StdFeature.from(varName, BoundedOrdinalVariable(3), data)    
  }
  
  def pyApply(inputIR: MatrixIR):RFModel  = {
    println(Pretty(inputIR))
    val mv = Interpret(inputIR)
    RFModel(mv)
  }
}

//class RfImportanceAnalysis(val hc: HailContext, importanceAnalysis: ImportanceAnalysis) {
//
//  val oobError: Double  = importanceAnalysis.rfModel.oobError
//  
//  def variantImportance(nLimit:Int = 1000): KeyTable  = {    
//    val importantVariants = importanceAnalysis.importantVariables(nLimit)
//      .map { case (label, importance) => Row(RfImportanceAnalysis.labelToVariant(label), importance) }
//    KeyTable(hc, hc.sc.parallelize(importantVariants), RfImportanceAnalysis.impSignature, Array("variant"))
//  }
//}
//
//object RfImportanceAnalysis {
//  
//  val defaultRFParams = RandomForestParams()
// 
//  val impSignature = TStruct(("variant", TVariant), ("importance",TDouble))
//   
//  def labelToVariant(label:String):Variant = {
//    val contiPositionRefAlt = label.split(":")
//    Variant(contiPositionRefAlt(0), contiPositionRefAlt(1).toInt, 
//        contiPositionRefAlt(2), contiPositionRefAlt(3))
//  }
//  
//  val schema: Type = TStruct(
//    ("giniImportance", TDouble)
//  )
//  
//  def apply(vds: VariantDataset, yExpr:String, nTrees:Int, mtryFraction:Option[Double], oob:Boolean, 
//        seed: Option[Long] , batchSize:Int):RfImportanceAnalysis = {
//    require(vds.wasSplit)
//    val featureSource = HailFeatureSource(vds)
//    val labelSource = HailLabelSource(vds, yExpr)
//    implicit val vsContext = HailContextAdapter(vds.hc)
//    new RfImportanceAnalysis(vds.hc, ImportanceAnalysis.fromParams(featureSource, labelSource, 
//        rfParams = RandomForestParams(
//              nTryFraction = mtryFraction.getOrElse(defaultRFParams.nTryFraction), 
//              seed =  seed.getOrElse(defaultRFParams.seed), 
//              oob = oob
//          ),
//          nTrees = nTrees,
//          batchSize = batchSize      
//        ))
//  }
//}