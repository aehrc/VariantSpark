package au.csiro.variantspark.hail.methods

import java.io.{FileOutputStream, OutputStreamWriter}

import au.csiro.pbdava.ssparkle.common.utils.LoanUtils
import au.csiro.variantspark.algo.{
  DefTreeRepresentationFactory,
  RandomForest,
  RandomForestModel,
  RandomForestParams,
  TreeFeature
}
import au.csiro.variantspark.data.{BoundedOrdinalVariable, Feature, StdFeature}
import au.csiro.variantspark.external.ModelConverter
import au.csiro.variantspark.utils.HdfsPath
import is.hail.annotations.Annotation
import is.hail.expr.ir.{Interpret, MatrixIR, MatrixValue, TableIR, TableLiteral, TableValue}
import is.hail.expr.types.virtual._
import is.hail.stats.RegressionUtils
import is.hail.utils._
import is.hail.variant._
import org.apache.hadoop.conf.Configuration
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.storage.StorageLevel
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.writePretty
import org.json4s.{Formats, NoTypeHints}

import scala.collection.IndexedSeq

/**
  * Initial implementation of RandomForst model for hail
  * @param mv MatrixValue with extracted fields of interests, currently it's assumed
  *           that the per sample dependent variable is named `e`
  *           while the dependent variable is named `y`
  * @param rfParams random forest parameters to use
  */
case class RFModel(mv: MatrixValue, rfParams: RandomForestParams) {

  val responseVarName: String = "y"
  val entryVarname: String = "e"

  // maintain the same key as in the original matrix
  private val key: IndexedSeq[String] = mv.typ.rowKey
  private val keySignature = mv.typ.rowKeyStruct

  // for now we need to assert that the MatrixValue
  // is actually indexed by the locus
  // TODO: otherwise I need some way to serialize and deserialize the keys
  // which may be possible in the future
  // one more reason to make this API work for genotypes only ...

  require(keySignature.fields.size == 2,
    "The key needs to be (for now): (locus<*>, alleles: array<str>)")
  require(keySignature.fields(0).typ.isInstanceOf[TLocus],
    s"The first field in key needs to be TLocus[*] but is ${keySignature.fields(0).typ}")
  require(keySignature.fields(1).typ.isOfType(TArray.apply(TString())),
    s"The second field in key needs to be TArray[String] but is ${keySignature.fields(1).typ}")

  // the result should keep the key + add importance related field
  lazy val sig: TStruct = keySignature.insertFields(Array(("importance", TFloat64())))

  lazy val rf: RandomForest = new RandomForest(rfParams)
  val featuresRDD: RDD[Feature] = mv.rvd.toRows.map(RFModel.rowToFeature)
  lazy val inputData: RDD[TreeFeature] =
    DefTreeRepresentationFactory.createRepresentation(featuresRDD.zipWithIndex())

  // the a stateful object
  // TODO: Maybe refactor to a helper object
  var rfModel: RandomForestModel = _
  var impVarBroadcast: Broadcast[Map[Long, Double]] = _

  def fitTrees(nTrees: Int = 500, batchSize: Int = 100) {

    // TODO: This only allows to replace the current model with a newly fitted one
    // We may want to be abel to the trees.

    releaseModelState()
    // These are currently obrained as doubles and converted to Int's needed by RandomForest
    // This is because getPhenosCovCompleteSamples only works on Double64 columns
    // This may be optimized in the future
    val (yMat, cov, completeColIdx) =
      RegressionUtils.getPhenosCovCompleteSamples(mv, Array(responseVarName), Array[String]())
    // completeColIdx are indexes of the complete samples.
    // These can be used to subsample the entry data
    // but for now let's just assume that there are no NAs in the labels (and or covariates).
    // TODO: allow for NAs in the labels and/or covariates
    require(completeColIdx.length == mv.nCols,
      "NAs are not currenlty supported in response variable. Filter the data first.")
    val labelVector = yMat(::, 0)
    // TODO: allow for multi class classification
    if (!labelVector.forall(yi => yi == 0d || yi == 1d)) {
      fatal(
          "For classification random forestlabel must be bool or numeric"
            + " with all present values equal to 0 or 1")
    }
    val labels = labelVector.map(_.toInt).toArray

    // now we somehow need to get to row data
    if (inputData.getStorageLevel == StorageLevel.NONE) {
      inputData.cache()
      val totalVariables = inputData.count()
      info(s"Loaded ${totalVariables} variables")
    }
    rfModel = rf.batchTrainTyped(inputData, labels, nTrees, batchSize)
  }

  def oobError: Double = rfModel.oobError

  def variableImportance: TableIR = {
    val brVarImp = importanceMapBroadcast
    val mapRDD = inputData.mapPartitions { it =>
      val varImp = brVarImp.value
      it.map(tf => RFModel.tfFeatureToImpRow(tf.label, varImp.getOrElse(tf.index, 0.0)))
    }
    TableLiteral(TableValue(sig, key, mapRDD))
  }

  def toJson(jsonFilename: String) {
    println(s"Saving model to: ${jsonFilename}")
    implicit val hadoopConf: Configuration = inputData.sparkContext.hadoopConfiguration
    implicit val formats: AnyRef with Formats = Serialization.formats(NoTypeHints)
    LoanUtils
      .withCloseable(new OutputStreamWriter(HdfsPath(jsonFilename).create())) { objectOut =>
        writePretty(new ModelConverter(Map.empty).toExternal(rfModel), objectOut)
      }
  }

  def release() {
    inputData.unpersist()
    releaseModelState()
  }

  private def importanceMapBroadcast: Broadcast[Map[Long, Double]] = {
    require(rfModel != null, "Traind the model first")
    if (impVarBroadcast != null) {
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

  def tfFeatureToImpRow(label: String, impValue: Double): Row = {
    val elements = label.split("_")
    val alleles = elements.drop(2).map(_.asInstanceOf[Annotation]).toIndexedSeq
    Row(Locus(elements(0), elements(1).toInt), alleles, impValue) // , elements.drop(2))
  }

  def rowToFeature(r: Row): Feature = {
    val locus = r.getAs[Locus](0)
    val varName =
      (Seq(locus.contig, locus.position.toString) ++ r.getSeq[String](1)).mkString("_")
    val data = r.getSeq[Row](2).map(_.getInt(0)).toArray
    StdFeature.from(varName, BoundedOrdinalVariable(3), data)
  }

  def pyApply(inputIR: MatrixIR, mTryFraction: Option[Double], oob: Boolean,
      minNodeSize: Option[Int], maxDepth: Option[Int], seed: Option[Int]): RFModel = {
    val mv = Interpret(inputIR)
    RFModel(mv,
      RandomForestParams.fromOptions(mTryFraction = mTryFraction, oob = Some(oob),
        minNodeSize = minNodeSize, maxDepth = maxDepth, seed = seed.map(_.longValue)))
  }
}
