package au.csiro.variantspark.cli

import au.csiro.pbdava.ssparkle.common.arg4j.{AppRunner, TestArgs}
import au.csiro.pbdava.ssparkle.common.utils.VersionInfo
import au.csiro.sparkle.common.args4j.ArgsApp

class VersionCmd extends ArgsApp with TestArgs /* with SparkApp */ {

  override def run(): Unit = {

    println(s"Version: ${VersionInfo.version}")
    println("Details:")
    VersionInfo.gitProperties.map({ case (k, v) => s"$k = $v" }).foreach(println)
  }

  override def testArgs: Array[String] = Array.empty
}

object VersionCmd {
  def main(args: Array[String]) {
    AppRunner.mains[VersionCmd](args)
  }
}
