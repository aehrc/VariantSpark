package au.csiro.pbdava.ssparkle.common.utils

object MiscUtils {
  def isDeveloperMode = System.getProperty("develMode", "false").toBoolean
}