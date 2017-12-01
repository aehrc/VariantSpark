package au.csiro.variantspark.algo.metrics

import au.csiro.variantspark.algo.AggregablePairwiseOperation
import au.csiro.variantspark.algo.PairwiseOperation

object EuclideanPairwiseMetric extends AggregablePairwiseOperation {
  def unitOp(b1:Byte,b2:Byte):Long = (b1-b2).toLong * (b1-b2).toLong
  override def finalOp(result:Array[Long]):Array[Double] = result.map(l => Math.sqrt(l.toDouble))
}

object ManhattanPairwiseMetric extends AggregablePairwiseOperation {
  def unitOp(b1:Byte,b2:Byte):Long = Math.abs(b1-b2).toLong
}

@deprecated
object BitwiseAndPairwiseRevMetric extends AggregablePairwiseOperation {
  def unitOp(b1:Byte,b2:Byte):Long = (b1 & b2).toLong
}

@deprecated
object MultiPairwiseRevMetric extends AggregablePairwiseOperation {
  def unitOp(b1:Byte,b2:Byte):Long = b1.toLong * b2.toLong;
}

object SharedAltAlleleCount extends AggregablePairwiseOperation {
  def unitOp(b1:Byte,b2:Byte):Long = Math.min(b1.toLong, b2.toLong)
}

object AtLeastOneSharedAltAlleleCount extends AggregablePairwiseOperation {
  def unitOp(b1:Byte,b2:Byte):Long = { 
    val noOfSharedAlt = Math.min(b1.toLong, b2.toLong)
    if (noOfSharedAlt > 0) 1 else 0
  }
}
