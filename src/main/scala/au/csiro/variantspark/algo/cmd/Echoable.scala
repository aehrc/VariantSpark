package au.csiro.variantspark.algo.cmd

import org.kohsuke.args4j.Option

trait Echoable {
  @Option(name="-v", required=false, usage="Be verbose", aliases=Array("--verbose") )
  val beVerbose:Boolean  = false

  @Option(name="-s", required=false, usage="Be silent", aliases=Array("--silent"))
  val beSilent:Boolean  = false
  
  
  def echo(msg: =>String) {
    if (!beSilent) {
      println(msg)
    }
  }

  def verbose(msg: =>String) {
    if (!beSilent && beVerbose) {
      println(msg)      
    }
  }
  
}