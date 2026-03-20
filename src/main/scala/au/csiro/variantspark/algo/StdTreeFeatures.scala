package au.csiro.variantspark.algo

import au.csiro.variantspark.data.{
  BoundedOrdinalVariable,
  BoundedNominalVariable,
  ByteArrayData,
  ContinuousVariable,
  Data,
  Feature,
  VariableType,
  VectorData
}
import org.apache.spark.mllib.linalg.Vectors
import au.csiro.variantspark.algo.split.JNaiveContinousIndexedSplitter
import au.csiro.variantspark.algo.split.JOrderedIndexedSplitter
import au.csiro.variantspark.algo.split.JOrderedFastIndexedSplitter
import au.csiro.variantspark.algo.split.JNominalClassificationIndexedSplitter
import au.csiro.variantspark.algo.split.JNominalClassificationFastIndexedSplitter
import au.csiro.variantspark.algo.split.JNominalRegressionIndexedSplitter
import au.csiro.variantspark.algo.split.JNominalRegressionFastIndexedSplitter

/** Verbatim tree representation for continuous variables. Stores continous data
  * in a {{Vector}} of doubles.
  */
class StdContinousTreeFeature(val label: String, val index: Long, continousData: Array[Double])
    extends TreeFeature {
  def variableType: VariableType = ContinuousVariable
  def toData: Data = VectorData(Vectors.dense(continousData))
  override def size: Int = continousData.length
  override def at(i: Int): Double = continousData(i)
  override def createSplitter(impCalc: IndexedSplitAggregator): IndexedSplitter =
    new JNaiveContinousIndexedSplitter(impCalc, continousData)
}

/** A representation for ordered factors with no more than 127 levels. Stored as
  * {{Array[Byte]}}
  */
class SmallOrderedTreeFeature(val label: String, val index: Long, orderedData: Array[Byte],
    nLevels: Int)
    extends TreeFeature with FastSplitterProvider {
  def variableType: VariableType = BoundedOrdinalVariable(nLevels)
  def toData: Data = ByteArrayData(orderedData)
  override def size: Int = orderedData.length
  override def at(i: Int): Double = orderedData(i).toDouble
  override def createSplitter(impCalc: IndexedSplitAggregator): IndexedSplitter =
    new JOrderedIndexedSplitter(impCalc, orderedData, nLevels)
  override def confusionSize: Int = nLevels
  override def createSplitter(impCalc: IndexedSplitAggregator,
      confusionAgg: LevelAggregator): IndexedSplitter =
    new JOrderedFastIndexedSplitter(confusionAgg, impCalc, orderedData, nLevels)
}

/** A representation for nominal factors with no more than 64 levels. Stored as
  * {{Array[Byte]}}. Limited to 64 levels because the split bitmask is a Long
  */
class SmallNominalTreeFeature(val label: String, val index: Long, nominalData: Array[Byte],
    nLevels: Int)
    extends TreeFeature with FastSplitterProvider {
  def variableType: VariableType = BoundedNominalVariable(nLevels)
  def toData: Data = ByteArrayData(nominalData)
  override def size: Int = nominalData.length
  override def at(i: Int): Double = nominalData(i).toDouble
  override def createSplitter(impCalc: IndexedSplitAggregator): IndexedSplitter =
    impCalc match {
      case _: ClassificationSplitAggregator =>
        new JNominalClassificationIndexedSplitter(impCalc, nominalData, nLevels)
      case _: RegressionSplitAggregator =>
        new JNominalRegressionIndexedSplitter(impCalc, nominalData, nLevels)
    }
  override def confusionSize: Int = nLevels
  override def createSplitter(impCalc: IndexedSplitAggregator,
      confusionAgg: LevelAggregator): IndexedSplitter =
    confusionAgg match {
      case ca: ClassificationLevelAggregator =>
        new JNominalClassificationFastIndexedSplitter(ca, impCalc, nominalData, nLevels)
      case ra: RegressionLevelAggregator =>
        new JNominalRegressionFastIndexedSplitter(ra, impCalc, nominalData, nLevels)
    }
}

// TODO[ContVariables]: Add support for other variable types (e.g. Factors) as well as fast
// indexed representation for continuous variables.

/** The default representation factory
  */
case object DefTreeRepresentationFactory extends TreeRepresentationFactory {
  def createRepresentation(f: Feature, index: Long): TreeFeature = {
    f.variableType match {
      case BoundedOrdinalVariable(nLevels) if nLevels < 127 =>
        new SmallOrderedTreeFeature(f.label, index, f.data.valueAsByteArray, nLevels)
      case BoundedNominalVariable(nLevels) if nLevels < 64 =>
        new SmallNominalTreeFeature(f.label, index, f.data.valueAsByteArray, nLevels)
      case ContinuousVariable =>
        new StdContinousTreeFeature(f.label, index, f.data.valueAsVector.toArray)
      case _ =>
        throw new IllegalArgumentException(s"Unsupported feature type ${f.variableType}")
    }
  }
}
