package au.csiro.variantspark.cli

import au.csiro.sparkle.common.args4j.ArgsApp
import au.csiro.sparkle.cmd.CmdApp
import org.kohsuke.args4j.Option
import au.csiro.pbdava.ssparkle.common.arg4j.AppRunner
import collection.JavaConverters._
import au.csiro.pbdava.ssparkle.common.arg4j.TestArgs
import au.csiro.pbdava.ssparkle.spark.SparkApp
import org.apache.hadoop.fs.FileSystem
import au.csiro.pbdava.ssparkle.common.utils.LoanUtils._
import org.apache.hadoop.fs.Path
import au.csiro.variantspark.input.VCFFeatureSource
import au.csiro.variantspark.input.VCFSource
import au.csiro.pbdava.ssparkle.common.utils.Timed


class FilterCmd extends ArgsApp with TestArgs with SparkApp {

  @Option(name="-if", required=true, usage="This is input files", aliases=Array("--input-files"))
  val inputFile:String = null

  @Option(name="-l", required=false)
  val limit:Int = 0

  @Override
  def testArgs = Array("-if", "data/small.vcf")  

  @Override
  def run():Unit = {
    implicit val fs = FileSystem.get(sc.hadoopConfiguration)  
    logDebug(s"Runing with filesystem: ${fs}, home: ${fs.getHomeDirectory}") 
    
    val vcfSource = VCFSource(sc.textFile(inputFile))
    val source  = VCFFeatureSource(vcfSource)
    val features = source.features().zipWithIndex().cache()
    val featureCount = features.count()
    println(s"No feautures: ${featureCount}")    
       
    val (sampleCount, sampleTime) = Timed.time(features.sample(false, 0.05).collect().size)
    println(s"No feautures sample: ${sampleCount}, time: ${sampleTime}")

    val (filterCount, filterTime) = Timed.time(features.filter(_ => Math.random() < 0.05).collect().size)
    println(s"No feautures filter: ${filterCount}, time: ${filterTime}") 

    val (setCount, setTime) = Timed.time{
      val indexed = Range(0, featureCount.toInt).filter(_ => Math.random() < 0.05).map(_.toLong).toSet
      val br_index = sc.broadcast(indexed)
      features.filter(t => br_index.value.contains(t._2)).collect().size
    }
    println(s"No feautures set: ${setCount}, time: ${setTime}") 
  }
}

object FilterCmd  {
  def main(args:Array[String]) {
    AppRunner.mains[FilterCmd](args)
  }
}
