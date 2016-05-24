package au.csiro.variantspark.cli

import au.csiro.sparkle.common.args4j.ArgsApp
import au.csiro.sparkle.cmd.CmdApp
import org.kohsuke.args4j.Option
import au.csiro.pbdava.ssparkle.common.arg4j.AppRunner



class TestCmd extends ArgsApp {

  @Option(name="-if", required=false)
  val inputFile:String = null

  @Option(name="-l", required=false)
  val limit:Int = 0

  
  @Override
  def run():Unit = {
    println("Hello Word: " + inputFile + ", " + limit)
  }
}

object TestCmd  {
  def main(args:Array[String]) {
    AppRunner.mains[TestCmd](args)
  }
}
