package au.csiro.variantspark.hail.methods

import java.io.OutputStreamWriter
import au.csiro.pbdava.ssparkle.common.utils.{LoanUtils, Logging}
import au.csiro.variantspark.algo.{
  DefTreeRepresentationFactory,
  RandomForest,
  RandomForestModel,
  RandomForestParams,
  TreeFeature
}
import au.csiro.variantspark.data.{BoundedOrdinalVariable, Feature, StdFeature}
import au.csiro.variantspark.external.ModelConverter
import au.csiro.variantspark.input.{
  DisabledImputationStrategy,
  ImputationStrategy,
  Missing,
  ModeImputationStrategy
}
import au.csiro.variantspark.utils.HdfsPath
import is.hail.annotations.Annotation
import is.hail.backend.spark.SparkBackend
import is.hail.expr.ir.{Interpret, MatrixIR, MatrixValue, TableIR, TableLiteral, TableValue}
import is.hail.stats.RegressionUtils
import is.hail.types.virtual._
import is.hail.utils.{ExecutionTimer, fatal}
import is.hail.variant._

import javax.annotation.Nullable
import org.apache.hadoop.conf.Configuration
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.storage.StorageLevel
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.writePretty
import org.json4s.{Formats, NoTypeHints}

import scala.collection.IndexedSeq

/**
  * Initial implementation of RandomForst model for hail
  * @param inputIR MatrixIR with extracted fields of interests, currently it's assumed
  *           that the per sample dependent variable is named `e`
  *           while the dependent variable is named `y`
  * @param rfParams random forest parameters to use
  */
case class RFModel(backend: SparkBackend, inputIR: MatrixIR, rfParams: RandomForestParams,
    imputationStrategy: Option[ImputationStrategy])
    extends Logging with AutoCloseable {

  val responseVarName: String = "y"
  val entryVarname: String = "e"

  // the a stateful object
  // TODO: Maybe refactor to a helper object
  // maintain the same key as in the original matrix
  var key: IndexedSeq[String] = _
  var keySignature: TStruct = _

  var rfModel: RandomForestModel = _
  var impVarBroadcast: Broadcast[Map[Long, Double]] = _
  var splitCountBroadcast: Broadcast[Map[Long, Long]] = _
  var inputData: RDD[TreeFeature] = _

  def fitTrees(nTrees: Int = 500, batchSize: Int = 100) {

    // TODO: This only allows to replace the current model with a newly fitted one
    // We may want to be abel to the trees.

    releaseModelState()
    rfModel = ExecutionTimer.logTime("RFModel.fitTrees") { timer =>
      backend.withExecuteContext(timer) { implicit ctx =>
        val tv = Interpret.apply(inputIR, ctx, true)
        val mv = tv.toMatrixValue(inputIR.typ.colKey)

        // maintain the same key as in the original matrix
        key = mv.typ.rowKey
        keySignature = mv.typ.rowKeyStruct

        // for now we need to assert that the MatrixValue
        // is actually indexed by the locus
        // TODO: otherwise I need some way to serialize and deserialize the keys
        // which may be possible in the future
        // one more reason to make this API work for genotypes only ...

        require(keySignature.fields.size == 2,
          "The key needs to be (for now): (locus<*>, alleles: array<str>)")
        require(keySignature.fields(0).typ.isInstanceOf[TLocus],
          s"The first field in key must be TLocus[*] but is ${keySignature.fields(0).typ}")
        require(keySignature.fields(1).typ == TArray(TString),
          s"The second field in key must be TArray[String] but is ${keySignature.fields(1).typ}")

        lazy val rf: RandomForest = new RandomForest(rfParams)

        // These are currently obrained as doubles and converted to Int's needed by RandomForest
        // This is because getPhenosCovCompleteSamples only works on Double64 columns
        // This may be optimized in the future
        val (yMat, cov, completeColIdx) =
          RegressionUtils.getPhenosCovCompleteSamples(mv, Array(mv.typ.colType.fieldNames.head),
            mv.typ.colType.fieldNames.tail)
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

        val covFeatures = mv.typ.colType.fieldNames.tail.toSeq.zipWithIndex
          .map {
            case (name, i) =>
              StdFeature.from(name, Vectors.dense(cov(::, i).toArray))
          }

        val covariateRDD = backend.sc.makeRDD(covFeatures)
        val genotypeRDD: RDD[Feature] =
          RFModel.mvToFeatureRDD(mv, imputationStrategy.getOrElse(DisabledImputationStrategy))

        val featuresRDD = genotypeRDD.union(covariateRDD)

        inputData = DefTreeRepresentationFactory.createRepresentation(featuresRDD.zipWithIndex())

        // now we somehow need to get to row data
        if (inputData.getStorageLevel == StorageLevel.NONE) {
          inputData.cache()
          val totalVariables = inputData.count()
          logInfo(s"Loaded ${totalVariables} variables")
        }
        rf.batchTrainTyped(inputData, labels, nTrees, batchSize)
      }
    }
  }

  def oobError: Double = rfModel.oobError

  def variableImportance: TableIR = {
    ExecutionTimer.logTime("RFModel.fitTrees") { timer =>
      backend.withExecuteContext(timer) { ctx =>
        // the result should keep the key + add importance related field
        val sig: TStruct =
          keySignature.insertFields(Array(("importance", TFloat64), ("splitCount", TInt64)))
        val brVarImp = importanceMapBroadcast
        val brSplitCount = splitCountMapBroadcast
        val mapRDD = inputData.mapPartitions { it =>
          val varImp = brVarImp.value
          val splitCount = brSplitCount.value

          it.filter ( tf => !tf.label.contains("cov__") )
            .map { tf =>
              RFModel.tfFeatureToImpRow(tf.label, varImp.getOrElse(tf.index, 0.0),
                splitCount.getOrElse(tf.index, 0L))
            }
        }
        TableLiteral(TableValue(ctx, sig, key, mapRDD))
      }
    }
  }

  def covariatesImportance: TableIR = {
    ExecutionTimer.logTime("RFModel.fitTrees") { timer =>
      backend.withExecuteContext(timer) { ctx =>
        // the result should keep the key + add importance related field
        val sig: TStruct = TStruct("covariate" -> TString, "importance" -> TFloat64,
          "splitCount" ->
            TInt64)
        val brVarImp = importanceMapBroadcast
        val brSplitCount = splitCountMapBroadcast
        val mapRDD = inputData.mapPartitions { it =>
          val varImp = brVarImp.value
          val splitCount = brSplitCount.value

          it.filter { tf => tf.label.contains("cov__") }
            .map { tf =>
              Row(tf.label.split("cov__")(1), varImp.getOrElse(tf.index, 0.0),
                splitCount.getOrElse(tf.index, 0L))
            }
        }
        TableLiteral(TableValue(ctx, sig, Array("covariate"), mapRDD))
      }
    }
  }

  def toJson(jsonFilename: String, resolveVarNames: Boolean) {
    println(s"Saving model to: ${jsonFilename}")
    implicit val hadoopConf: Configuration = inputData.sparkContext.hadoopConfiguration
    implicit val formats: AnyRef with Formats = Serialization.formats(NoTypeHints)

    val variableIndex = if (resolveVarNames) {
      val brVarImp = importanceMapBroadcast
      inputData
        .mapPartitions({ it =>
          val impVariableSet = brVarImp.value.keySet
          it.filter(t => impVariableSet.contains(t.index))
            .map(f => (f.index, f.label))
        })
        .collectAsMap()
        .toMap
    } else {
      Map.empty[Long, String]
    }
    LoanUtils
      .withCloseable(new OutputStreamWriter(HdfsPath(jsonFilename).create())) { objectOut =>
        writePretty(new ModelConverter(variableIndex).toExternal(rfModel), objectOut)
      }
  }

  def release() {
    releaseModelState()
  }

  private def importanceMapBroadcast: Broadcast[Map[Long, Double]] = {
    require(rfModel != null, "Train the model first")
    if (impVarBroadcast != null) {
      impVarBroadcast
    } else {
      impVarBroadcast = backend.sparkSession.sparkContext.broadcast(rfModel.variableImportance)
      impVarBroadcast
    }
  }

  private def splitCountMapBroadcast: Broadcast[Map[Long, Long]] = {
    require(rfModel != null, "Train the model first")
    if (splitCountBroadcast != null) {
      splitCountBroadcast
    } else {
      splitCountBroadcast = backend.sparkSession.sparkContext.broadcast(
          rfModel.variableSplitCount)
      splitCountBroadcast
    }
  }

  private def releaseModelState() {
    if (impVarBroadcast != null) {
      impVarBroadcast.destroy()
    }
    if (splitCountBroadcast != null) {
      splitCountBroadcast.destroy()
    }
    if (inputData != null) {
      inputData.unpersist()
    }
    inputData = null
    impVarBroadcast = null
    rfModel = null
    key = null
    keySignature = null
  }

  override def close(): Unit = {
    release()
  }
}

object RFModel {

  def tfFeatureToImpRow(label: String, impValue: Double, splitCount: Long): Row = {
    val elements = label.split("_")
    val alleles = elements.drop(2).map(_.asInstanceOf[Annotation]).toIndexedSeq
    Row(Locus(elements(0), elements(1).toInt), alleles, impValue, splitCount)
  }

  def mvToFeatureRDD(mv: MatrixValue, imputationStrategy: ImputationStrategy): RDD[Feature] = {
    // toRows <==> to external rows as far as I understand
    // which will allow the RDD to be used outside of the
    // execution context (which is what we want here)
    mv.rvd.toRows.map(rowToFeature(_, imputationStrategy))
  }

  def rowToFeature(r: Row, is: ImputationStrategy): Feature = {
    val locus = r.getAs[Locus](0)
    val varName =
      (Seq(locus.contig, locus.position.toString) ++ r.getSeq[String](1)).mkString("_")
    val data = r
      .getSeq[Row](2)
      .map(g => if (!g.isNullAt(0)) g.getInt(0).toByte else Missing.BYTE_NA_VALUE)
      .toArray
    StdFeature.from(varName, BoundedOrdinalVariable(3), is.impute(data))
  }

  def imputationFromString(imputationType: String): ImputationStrategy = {
    imputationType match {
      case "mode" => ModeImputationStrategy(3)
      case _ =>
        throw new IllegalArgumentException(
            "Unknown imputation type: '" + imputationType + "'. Valid types are: 'mode'")
    }

  }

  def optionFromNullable[J, S](jValue: J)(implicit conversion: J => S): Option[S] =
    if (jValue == null) None else Some(conversion(jValue))

  def pyApply(backend: SparkBackend, inputIR: MatrixIR, @Nullable mTryFraction: java.lang.Double,
      oob: Boolean, @Nullable minNodeSize: java.lang.Integer,
      @Nullable maxDepth: java.lang.Integer, @Nullable seed: java.lang.Integer,
      @Nullable imputationType: String = null): RFModel = {
    var rfParams = RandomForestParams.fromOptions(mTryFraction = optionFromNullable(mTryFraction),
      oob = Some(oob), minNodeSize = optionFromNullable(minNodeSize),
      maxDepth = optionFromNullable(maxDepth), seed = optionFromNullable(seed).map(_.longValue))
    RFModel(backend, inputIR, rfParams, Option(imputationType).map(imputationFromString))
  }
}
