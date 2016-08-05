package au.csiro.variantspark.cli

import au.csiro.sparkle.common.args4j.ArgsApp
import au.csiro.sparkle.cmd.CmdApp
import org.kohsuke.args4j.Option
import au.csiro.pbdava.ssparkle.common.arg4j.AppRunner
import collection.JavaConverters._
import au.csiro.pbdava.ssparkle.common.arg4j.TestArgs


class TestCmd extends ArgsApp with TestArgs {

  @Option(name="-if", required=false, usage="This is input files", aliases=Array("--input-files"))
  val inputFile:String = null

  @Option(name="-l", required=true)
  val limit:Int = 0

  @Override
  def testArgs = Array("-l", "10")
  
  @Override
  def run():Unit = {
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
