package au.csiro.pbdava.ssparkle.common.arg4j

import au.csiro.sparkle.common.args4j.ArgsApp
import au.csiro.sparkle.cmd.CmdApp


trait AppRunner2[T<:CmdApp] {
   def main[T:Manifest](args:Array[String]) {
    CmdApp.runApp(args, manifest[T].erasure.newInstance().asInstanceOf[CmdApp])
  }
}