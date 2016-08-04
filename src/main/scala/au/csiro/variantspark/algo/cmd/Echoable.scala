package au.csiro.variantspark.algo.cmd

import org.kohsuke.args4j.Option

trait Echoable {
  @Option(name="-v", required=false, usage="Be verbose", aliases=Array("--verbose") )
  val beVerbose:Boolean  = false

  @Option(name="-s", required=false, usage="Be silent", aliases=Array("--silent"))
  val beSilent:Boolean  = false
  
  def isSilent = beSilent
  def isEcho = !isSilent
  def isVerbose = !beSilent && beVerbose
  
  def echo(msg: =>String) {
    if (isEcho) {
      println(msg)
    }
  }

  def verbose(msg: =>String) {
    if (isVerbose) {
      println(msg)      
    }
  }
  
}