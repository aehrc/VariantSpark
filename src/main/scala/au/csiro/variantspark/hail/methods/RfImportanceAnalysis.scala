package au.csiro.variantspark.hail.methods

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
import org.apache.spark.storage.StorageLevel

case class RFModel(mv:MatrixValue, rfParams: RandomForestParams) { 
 
  // for now we need to assert that the MatrixValue 
  // is actually indexed by the locus 
  // TODO: otherwise I need some way to serialize and deserialize the keys 
  // which may be possible in the future
  // one more reason to make this API work for genotypes only ... 
  
  // maintain the same key as in the original matrix
  lazy private val keys: IndexedSeq[String] = mv.typ.rowKey
  // the result should keep the key + importance field
  lazy val sig = mv.typ.rowKeyStruct.insertFields(Array(("importance", TFloat64())))
      
  lazy val rf = new  RandomForest(rfParams)
  val featuresRDD = mv.rvd.toRows.map(RFModel.rowToFeature)   
  lazy val inputData:RDD[TreeFeature] = DefTreeRepresentationFactory.createRepresentation(featuresRDD.zipWithIndex())

  var rfModel:RandomForestModel = null
  
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
    if (inputData.getStorageLevel == StorageLevel.NONE) {
      inputData.cache() 
      val totalVariables = inputData.count()
      println(s"Loaded ${totalVariables} variables")
      inputData.take(4).foreach(println _)
    }
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
  
  def release() {
    inputData.unpersist()
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
  
  def pyApply(inputIR: MatrixIR, mTryFraction:Option[Double], oob:Boolean, minNodeSize:Option[Int], 
      maxDepth:Option[Int],
      seed:Option[Int]):RFModel  = {
    //println(Pretty(inputIR))
    val mv = Interpret(inputIR)
    RFModel(mv, RandomForestParams.fromOptions(mTryFraction=mTryFraction, oob=Some(oob), 
        minNodeSize = minNodeSize, maxDepth = maxDepth,
        seed=seed.map(_.longValue)))
  }
}