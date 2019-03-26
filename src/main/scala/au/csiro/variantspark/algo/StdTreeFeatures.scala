package au.csiro.variantspark.algo

import au.csiro.variantspark.data.ContinuousVariable
import au.csiro.variantspark.data.VectorData
import org.apache.spark.mllib.linalg.Vectors
import au.csiro.variantspark.algo.split.JNaiveContinousIndexedSplitter
import au.csiro.variantspark.data.BoundedOrdinalVariable
import au.csiro.variantspark.data.ByteArrayData
import au.csiro.variantspark.algo.split.JOrderedIndexedSplitter
import au.csiro.variantspark.algo.split.JOrderedFastIndexedSplitter
import au.csiro.variantspark.data.Feature



/**
 * Verbatim tree representation for continuous variables.
 * Stores continous data in a {{Vector}} of doubles.
 */
class StdContinousTreeFeature(val label:String, val index:Long, continousData:Array[Double]) extends TreeFeature {
  def variableType = ContinuousVariable
  def toData = new VectorData(Vectors.dense(continousData))
  override def size = continousData.size
  override def at(i:Int) = continousData(i)
  override def createSplitter(impCalc:IndexedSplitAggregator) = new JNaiveContinousIndexedSplitter(impCalc,continousData)
}

/**
 * A representation for ordered factors with no more than 127 levels. 
 * Stored as {{Array[Byte]}}
 */
class SmallOrderedTreeFeature(val label:String, val index:Long, orderedData:Array[Byte], nLevels:Int) extends TreeFeature with  FastSplitterFactory {
  def variableType = BoundedOrdinalVariable(nLevels)
  def toData = new ByteArrayData(orderedData) 
  override def size = orderedData.size
  override def at(i:Int) = orderedData(i).toDouble
  override def createSplitter(impCalc:IndexedSplitAggregator) = new JOrderedIndexedSplitter(impCalc, orderedData, nLevels)
  override def confusionSize = nLevels
  override def createSplitter(impCalc:IndexedSplitAggregator, confusionAgg:ConfusionAggregator) =  new JOrderedFastIndexedSplitter(confusionAgg, impCalc, orderedData, nLevels) 
}

//TODO[ContVariables]: Add support for other variable types (e.g. Factors) as well as fast indexed representation for continuous variables.

/**
 * The default representation factory
 */
case object DefTreeRepresentationFactory extends TreeRepresentationFactory {
  def createRepresentation(f:Feature, index:Long):TreeFeature  = {
    f.variableType match {
      case BoundedOrdinalVariable(nLevels) if (nLevels < 127) => new  SmallOrderedTreeFeature(f.label, index, f.data.valueAsByteArray, nLevels)
      case ContinuousVariable => new StdContinousTreeFeature(f.label, index, f.data.valueAsVector.toArray) 
      case _ => throw new IllegalArgumentException(s"Unsupported feature type ${f.variableType}")
    }
  }
}