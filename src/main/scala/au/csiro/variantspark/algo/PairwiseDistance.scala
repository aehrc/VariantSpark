package au.csiro.variantspark.algo

import breeze.linalg.DenseMatrix
import org.apache.spark.rdd.RDD
import org.apache.spark.AccumulableParam

trait PairwiseMetric extends Serializable {
  def unitOp(b1:Byte,b2:Byte):Long
  def finalOp(result:Array[Long]):Array[Double] = result.map(_.toDouble)
}


object EucledianPairwiseMetric extends PairwiseMetric {
  def unitOp(b1:Byte,b2:Byte):Long = (b1-b2).toLong * (b1-b2).toLong
  override def finalOp(result:Array[Long]):Array[Double] = result.map(l => Math.sqrt(l.toDouble))
}

object ManhattanPairwiseMetric extends PairwiseMetric {
  def unitOp(b1:Byte,b2:Byte):Long = Math.abs(b1-b2).toLong
}

object BitwiseAndPairwiseRevMetric extends PairwiseMetric {
  def unitOp(b1:Byte,b2:Byte):Long = (b1 & b2).toLong
}

object MultiPairwiseRevMetric extends PairwiseMetric {
  def unitOp(b1:Byte,b2:Byte):Long = b1.toLong * b2.toLong;
}

case class PairWiseAggregator(val metric:PairwiseMetric) {
    
   def seqOp(result: Array[Long], t: Array[Byte]): Array[Long] = {
      var index = 0
      for(r <- Range(1, t.length); c <- Range(0, r))  {
        result(index) += metric.unitOp(t(r),t(c))
        index += 1
      }
        result      
    }
   
  def combOp(r1: Array[Long], r2: Array[Long]): Array[Long] = {
    for(i <- Range(0, r1.length)) { r1(i) += r2(i)}
    r1
  }
  
}


class PairwiseDistance(val metric:PairwiseMetric) {
  
  /**
   * Computes lower triangular part of the pairwise distance matrix with Euclidean distance
   */
  def compute(data: RDD[Array[Byte]]):Array[Double] = {
    val noOfSamples = data.first.length
    // we need to allocate array for lower triangular matrix
    // size n*(n-1) /2    
    val outputMatSize = noOfSamples*(noOfSamples-1)/2    
    val zeroVal = Array.fill(outputMatSize)(0L)
    val pwAggregator = PairWiseAggregator(metric)
    val resultAsLong = data.treeAggregate(zeroVal)(pwAggregator.seqOp, pwAggregator.combOp)
    metric.finalOp(resultAsLong)    
  }
}


object PairwiseDistance {
   
  def apply() = new PairwiseDistance(EucledianPairwiseMetric)

  def lowerTriangToMatrix(lowerTriang:Array[Double], matrixSize1D:Int):DenseMatrix[Double] = {
    // need to infer matrix size from lowerTriangSize
    assert(lowerTriang.length == (matrixSize1D-1)*matrixSize1D / 2, "Correct dimension passed")
    DenseMatrix.tabulate(matrixSize1D,matrixSize1D) { case (r,c) => 
          if (r == c) 0 else if (c < r ) lowerTriang((r-1) * r/2 + c) else lowerTriang( (c-1)*c/2 + r) 
    }
  }  
}