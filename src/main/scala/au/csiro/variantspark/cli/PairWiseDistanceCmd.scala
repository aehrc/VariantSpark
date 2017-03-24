package au.csiro.variantspark.cli

import java.io.FileInputStream
import java.io.ObjectInputStream

import scala.collection.JavaConverters._

import org.apache.commons.lang3.builder.ToStringBuilder
import org.apache.spark.Logging
import org.kohsuke.args4j.Option

import au.csiro.pbdava.ssparkle.common.arg4j.AppRunner
import au.csiro.pbdava.ssparkle.common.arg4j.TestArgs
import au.csiro.pbdava.ssparkle.common.utils.LoanUtils
import au.csiro.sparkle.common.args4j.ArgsApp
import au.csiro.variantspark.cmd.EchoUtils._
import au.csiro.variantspark.cmd.Echoable
import au.csiro.variantspark.utils.IndexedRDDFunction._
import au.csiro.variantspark.utils.VectorRDDFunction._
import au.csiro.variantspark.utils.defRng
import com.github.tototoshi.csv.CSVWriter
import au.csiro.variantspark.algo.RandomForestModel
import au.csiro.variantspark.cli.args.FeatureSourceArgs
import au.csiro.pbdava.ssparkle.spark.SparkUtils
import org.apache.spark.serializer.JavaSerializer
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap
import org.apache.spark.Accumulator
import org.apache.spark.Accumulable
import org.apache.spark.AccumulableParam
import java.io.FileOutputStream
import java.io.ObjectOutputStream
import breeze.linalg.DenseMatrix
import java.io.File
import breeze.linalg.DenseVector

// TODO:
// need to figure out upper triangular index transformation
// 

object PariWiseAccumulator extends AccumulableParam[Array[Long], Array[Byte]] {
  
  // adding to lower traingular matrix
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

class PairWiseDistanceCmd extends ArgsApp with FeatureSourceArgs with Logging with TestArgs {

  @Option(name="-of", required=true, usage="Path to output distance file", aliases=Array("--output-file"))
  val outputFile:String = null

  @Override
  def testArgs = Array("-if", "data/chr22_1000.vcf", 
      "-of", "target/ch22-disc.csv"
      )
      
  @Override
  def run():Unit = {
    logInfo("Running with params: " + ToStringBuilder.reflectionToString(this))
    echo(s"Calculating pair wise distance")
    
    val data = featureSource.features().map(_.values)
    
    // a naive implementation would be to just aggregate (but perhaps it will work)
    // but why not (?)
    val noOfSamples = data.first.length
    // we need to allocate array for lower traingulat matrix 
    // size n*(n-1) /2 
    val outputMatSize = noOfSamples*(noOfSamples-1)/2
    val pairWiseAccumulator = sc.accumulable(Array.fill(outputMatSize)(0L))(PariWiseAccumulator)
    featureSource.features().map(_.values).foreach(pairWiseAccumulator.add(_))
    val resultAsLong = pairWiseAccumulator.value
    val result = resultAsLong.map(l => Math.sqrt(l.toDouble))
    
    val resultAsMatrix = DenseMatrix.tabulate(noOfSamples,noOfSamples) { case (r,c) => 
          if (r == c) 0 else if (c < r ) result((r-1) * r/2 + c) else result( (c-1)*c/2 + r) 
    }
    
    // format this in a form of a matrix
    // fill the missin entires 
        
    val sampleNames = featureSource.sampleNames
    LoanUtils.withCloseable(CSVWriter.open(new File(outputFile))) { writer =>
      writer.writeRow("" :: sampleNames)
      // since the matrix is symetric does not matter that we output coumns as rows
      Range(0, noOfSamples).foreach(i => writer.writeRow(sampleNames(i) :: resultAsMatrix(::,i).toArray.toList))
    }
    
  }  
}

object PairWiseDistanceCmd  {
  def main(args:Array[String]) {
    AppRunner.mains[PairWiseDistanceCmd](args)
  }
}
