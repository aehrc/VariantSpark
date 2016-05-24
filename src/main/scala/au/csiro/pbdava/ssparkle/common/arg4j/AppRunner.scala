package au.csiro.pbdava.ssparkle.common.arg4j

import au.csiro.sparkle.common.args4j.ArgsApp
import au.csiro.sparkle.cmd.CmdApp

trait AppRunner[T<:ArgsApp]

object AppRunner {
  
  def mains[T<:CmdApp](args:Array[String])(implicit m:Manifest[T])  {
    CmdApp.runApp(args,m.runtimeClass.newInstance().asInstanceOf[CmdApp])
  }

}