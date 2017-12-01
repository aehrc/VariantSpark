package au.csiro.variantspark.algo

import breeze.linalg.DenseMatrix
import breeze.linalg._
import org.apache.spark.rdd.RDD
import org.apache.spark.AccumulableParam
import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix
import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix
import org.apache.spark.mllib.linalg.distributed.IndexedRow
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.SparkContext

case class PairWiseAggregator(val metric:AggregablePairwiseOperation) {
    
   def seqOp(result: Array[Long], t: Array[Byte]): Array[Long] = {
      var index = 0
      for(r <- Range(0, t.length); c <- Range(0, r + 1))  {
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

class UpperTriangMatrix(val value: Array[Double]) extends AnyVal {
  def toMatrix:DenseMatrix[Double] =  PairwiseOperation.upperTriangWithDiagToMatrix(value)
  def toArray:Array[Array[Double]] = PairwiseOperation.upperTriangWithDiagToArray(value)
  def toIndexedRowMatrix(sc:SparkContext):IndexedRowMatrix =  {
    val rows = toArray.map(row => Vectors.dense(row)).zipWithIndex.map { 
        case (v,i) => IndexedRow(i.toLong, v) 
    }
    new IndexedRowMatrix(sc.parallelize(rows.toSeq))
  }
}

trait PairwiseOperation {
  def compute(data: RDD[Array[Byte]]):UpperTriangMatrix
}

trait AggregablePairwiseOperation extends PairwiseOperation with Serializable {
  def unitOp(b1:Byte,b2:Byte):Long
  def finalOp(result:Array[Long]):Array[Double] = result.map(_.toDouble)
  
  def compute(data: RDD[Array[Byte]]):UpperTriangMatrix = {
    val noOfSamples = data.first.length
    // we need to allocate array for upper triangular matrix with diagonal
    // size n*(n+1) /2    
    val outputMatSize = noOfSamples*(noOfSamples+1)/2    
    val zeroVal = Array.fill(outputMatSize)(0L)
    val pwAggregator = PairWiseAggregator(this)
    val resultAsLong = data.treeAggregate(zeroVal)(pwAggregator.seqOp, pwAggregator.combOp)
    new UpperTriangMatrix(finalOp(resultAsLong))  
  }  
}

object PairwiseOperation {
  
  def sizeFromUpperDiagLenght(upperDiagSize: Int) = {
    val size = ((Math.sqrt(1.0 + 8* upperDiagSize ) - 1.0) / 2.0).toInt
    require(upperDiagSize ==  size*(size + 1) / 2)
    size
  }
  
  def upperTriangWithDiagToArray(upperTriang:Array[Double]):Array[Array[Double]] = {
    val matrixSize1D = sizeFromUpperDiagLenght(upperTriang.length)
    // need to infer matrix size from lowerTriangSize
    Range(0,matrixSize1D).map { r =>
      Range(0,matrixSize1D).map(c => if (c >= r ) upperTriang(c*(c+1)/2 + r) else upperTriang(r*(r+1)/2 + c)).toArray
    }.toArray
  }  
  
  def upperTriangWithDiagToMatrix(upperTriang:Array[Double]):DenseMatrix[Double] = {
    val matrixSize1D = sizeFromUpperDiagLenght(upperTriang.length)
    // need to infer matrix size from lowerTriangSize
    DenseMatrix.tabulate(matrixSize1D,matrixSize1D) { case (r,c) => 
      if (c >= r ) upperTriang(c*(c+1)/2 + r) else upperTriang(r*(r+1)/2 + c)
    }
  }  
}