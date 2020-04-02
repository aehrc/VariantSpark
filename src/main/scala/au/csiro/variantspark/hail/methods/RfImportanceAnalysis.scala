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
import is.hail.expr.types.virtual.Field
import org.apache.spark.broadcast.Broadcast



/**
 * Initial implementation of RandomForst model for hail
 * @param mv MatrixValue with extracted fields of interests, currently it's assumed that the per sample dependent variable is named `e`
 * 					while the dependent variable is named `y`
 * @rfParams random forest parameters to use
 */
case class RFModel(mv:MatrixValue, rfParams: RandomForestParams) { 
  
  val responseVarName = "y"
  val entryVarname = "e"
  
  
  // maintain the same key as in the original matrix
  private val key: IndexedSeq[String] = mv.typ.rowKey
  private val keySignature = mv.typ.rowKeyStruct

  // for now we need to assert that the MatrixValue 
  // is actually indexed by the locus 
  // TODO: otherwise I need some way to serialize and deserialize the keys 
  // which may be possible in the future
  // one more reason to make this API work for genotypes only ... 

  require(keySignature.fields.size == 2, "The key needs to be (for now): (locus<*>, alleles: array<str>)")
  require(keySignature.fields(0).typ.isInstanceOf[TLocus], 
    s"The first field in key needs to be TLocus[*] but is ${keySignature.fields(0).typ}")
  require(keySignature.fields(1).typ.isOfType(TArray.apply(TString())),
    s"The second field in key needs to be TArray[String] but is ${keySignature.fields(1).typ}")
  
//  require(keySignature == TStruct(("locus", TLocus(ReferenceGenome.GRCh37)),	
//      ("alleles", TArray.apply(TString()))), "The key needs to be (for now): (locus<GRCh37>, alleles: array<str>)")
  
  // the result should keep the key + add importance related field
  lazy val sig = keySignature.insertFields(Array(("importance", TFloat64())))
 
  lazy val rf = new  RandomForest(rfParams)
  val featuresRDD = mv.rvd.toRows.map(RFModel.rowToFeature)   
  lazy val inputData:RDD[TreeFeature] = DefTreeRepresentationFactory.createRepresentation(featuresRDD.zipWithIndex())

  // the a stateful object
  // TODO: Maybe refactor to a helper object
  var rfModel:RandomForestModel = null
  var impVarBroadcast:Broadcast[Map[Long, Double]] = null
  
  def fitTrees(nTrees:Int = 500, batchSize:Int = 100)  {
     
    // TODO: This only allows to replace the current model with a newly fitted one
    // We may want to be abel to the trees.
    
    releaseModelState()
    // These are currently obrained as doubles and converted to Int's needed by RandomForest
    // This is because getPhenosCovCompleteSamples only works on Double64 columns 
    // This may be optimized in the future
    val (yMat, cov, completeColIdx) = RegressionUtils.getPhenosCovCompleteSamples(mv, Array(responseVarName), Array[String]())
    // completeColIdx are indexes of the complete samples. These can be used to subsample the entry data
    // but for now let's just assume that there are no NAs in the labels (and or covariates).
    // TODO: allow for NAs in the labels and/or covariates
    require(completeColIdx.length == mv.nCols, "NAs are not currenlty supported in response variable. Filter the data first.")
    val labelVector = yMat(::, 0)
    // TODO: allow for multi class classification
    if (!labelVector.forall(yi => yi == 0d || yi == 1d))
        fatal(s"For classification random forestlabel must be bool or numeric with all present values equal to 0 or 1")
    val labels = labelVector.map(_.toInt).toArray
    
    // now we somehow need to get to row data
    if (inputData.getStorageLevel == StorageLevel.NONE) {
      inputData.cache() 
      val totalVariables = inputData.count()
      info(s"Loaded ${totalVariables} variables")
    }
    rfModel = rf.batchTrainTyped(inputData, labels, nTrees, batchSize)
  }
  
  def oobError:Double = rfModel.oobError

  def variableImportance:TableIR = { 
    val brVarImp = importanceMapBroadcast
    val mapRDD  = inputData.mapPartitions { it =>
      val varImp = brVarImp.value
      it.map(tf => RFModel.tfFeatureToImpRow(tf.label, varImp.getOrElse(tf.index, 0.0)))
    }
    TableLiteral(TableValue(sig, key, mapRDD))
  }
  
  def release() {
    inputData.unpersist()
    releaseModelState()
  }
  
  private def importanceMapBroadcast:Broadcast[Map[Long, Double]] = {
    require(rfModel != null, "Traind the model first")
    if (impVarBroadcast!= null) { 
      impVarBroadcast 
    } else {
      impVarBroadcast = inputData.sparkContext.broadcast(rfModel.variableImportance)
      impVarBroadcast
    }
  }
  
  private def releaseModelState() {
    if (impVarBroadcast != null) {
      impVarBroadcast.destroy()
    }
    impVarBroadcast = null
    rfModel = null
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
    val mv = Interpret(inputIR)
    RFModel(mv, RandomForestParams.fromOptions(mTryFraction=mTryFraction, oob=Some(oob), 
        minNodeSize = minNodeSize, maxDepth = maxDepth,
        seed=seed.map(_.longValue)))
  }
}