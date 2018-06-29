package au.csiro.variantspark.cli

import java.io.FileInputStream
import java.io.ObjectInputStream

import scala.collection.JavaConverters._

import org.apache.commons.lang3.builder.ToStringBuilder
import au.csiro.pbdava.ssparkle.common.utils.Logging
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

object IndexAccumulator extends AccumulableParam[Long2ObjectOpenHashMap[String], (Long,String)] {
  def addAccumulator(r: Long2ObjectOpenHashMap[String], t: (Long, String)): Long2ObjectOpenHashMap[String] = {
    r.put(t._1, t._2)
    r
  }
  
  def addInPlace(r1: Long2ObjectOpenHashMap[String], r2: Long2ObjectOpenHashMap[String]): Long2ObjectOpenHashMap[String] = {
    r1.putAll(r2)
    r1
  }

  def zero(initialValue: Long2ObjectOpenHashMap[String]): Long2ObjectOpenHashMap[String] = {
    initialValue
  }
}

class BuildVarIndexCmd extends ArgsApp with FeatureSourceArgs with Logging with TestArgs {

  @Option(name="-oi", required=true, usage="Path to output index file", aliases=Array("--output-index"))
  val outputIndex:String = null

  @Override
  def testArgs = Array("-if", "data/chr22_1000.vcf", 
      "-oi", "target/ch22-idx.ser"
      )
      
  @Override
  def run():Unit = {
    logInfo("Running with params: " + ToStringBuilder.reflectionToString(this))
    echo(s"Building full variable index")
    
    val indexAccumulator = sc.accumulable(new Long2ObjectOpenHashMap[String]())(IndexAccumulator)
    featureSource.features().zipWithIndex().map(t => (t._2, t._1.label)).foreach(indexAccumulator.add(_))
    
    val index = indexAccumulator.value
    
    echo(s"Saving index of ${index.size()} variables to: ${outputIndex}")
    LoanUtils.withCloseable(new ObjectOutputStream(new FileOutputStream(outputIndex))) { objectOut =>
        objectOut.writeObject(index)
    }
  }  
}

object BuildVarIndexCmd  {
  def main(args:Array[String]) {
    AppRunner.mains[BuildVarIndexCmd](args)
  }
}
