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


class TestCmd extends ArgsApp with TestArgs with SparkApp {

  @Option(name="-if", required=false, usage="This is input files", aliases=Array("--input-files"))
  val inputFile:String = null

  @Option(name="-l", required=false)
  val limit:Int = 0

  @Override
  def testArgs = Array("-l", "10")
  
  @Override
  def run():Unit = {
    implicit val fs = FileSystem.get(sc.hadoopConfiguration)  
    logDebug(s"Runing with filesystem: ${fs}, home: ${fs.getHomeDirectory}") 
    println("Hello Word: " + inputFile + ", " + limit)
    println("Properties: ")
    System.getProperties().asScala.foreach(k => println(k))
  }
}

object TestCmd  {
  def main(args:Array[String]) {
    AppRunner.mains[TestCmd](args)
  }
}
