package au.csiro.pbdava.ssparkle.common.arg4j

import au.csiro.sparkle.common.args4j.ArgsApp
import au.csiro.sparkle.cmd.CmdApp
import au.csiro.pbdava.ssparkle.common.utils.MiscUtils

trait AppRunner[T <: ArgsApp]

object AppRunner {

  def mains[T <: CmdApp](args: Array[String])(implicit m: Manifest[T]) {
    val app = m.runtimeClass.newInstance().asInstanceOf[CmdApp]
    val actualArgs =
      if (args.length == 0 && app.isInstanceOf[TestArgs] && MiscUtils.isDeveloperMode)
        app.asInstanceOf[TestArgs].testArgs
      else args
    CmdApp.runApp(actualArgs, app)
  }

}
