package au.csiro.variantspark.algo

import breeze.linalg.DenseMatrix
import org.apache.spark.rdd.RDD
import org.apache.spark.AccumulableParam


object PairWiseAccumulator extends AccumulableParam[Array[Long], Array[Byte]] {

  // adding to lower triangular matrix
  def addAccumulator(result: Array[Long], t: Array[Byte]): Array[Long] = {
    var index = 0
    for(r <- Range(1, t.length); c <- Range(0, r))  {
      result(index) += (t(r)  - t(c)) * (t(r)  - t(c))
      index += 1
    }
    result
  }

  def addInPlace(r1: Array[Long], r2: Array[Long]): Array[Long] = {
    for(i <- Range(0, r1.length)) { r1(i) += r2(i)}
    r1
  }

  def zero(initialValue: Array[Long]): Array[Long] = {
    initialValue
  }
}

class PairwiseDistance {

  /**
   * Computes lower triangular part of the pairwise distance matrix with Euclidean distance
   */
  def compute(data: RDD[Array[Byte]]):Array[Double] = {
    val noOfSamples = data.first.length
    // we need to allocate array for lower triangular matrix
    // size n*(n-1) /2
    val outputMatSize = noOfSamples*(noOfSamples-1)/2
    val pairWiseAccumulator = data.sparkContext.accumulable(Array.fill(outputMatSize)(0L))(PairWiseAccumulator)
    data.foreach(pairWiseAccumulator.add(_))
    val resultAsLong = pairWiseAccumulator.value
    resultAsLong.map(l => Math.sqrt(l.toDouble))    
  }
  
  
}
object PairwiseDistance {
  
  def apply() = new PairwiseDistance()
 
  def lowerTraingToMatrix(lowerTriang:Array[Double], matrixSize1D:Int):DenseMatrix[Double] = {
    // need to infer matrix size from lowerTriangSize
    assert(lowerTriang.length == (matrixSize1D-1)*matrixSize1D / 2, "Correct dimension passed")
    DenseMatrix.tabulate(matrixSize1D,matrixSize1D) { case (r,c) => 
          if (r == c) 0 else if (c < r ) lowerTriang((r-1) * r/2 + c) else lowerTriang( (c-1)*c/2 + r) 
    }
  }
}