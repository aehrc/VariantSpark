package au.csiro.pbdava.ssparkle.common.utils

object MiscUtils {
  def isDeveloperMode: Boolean = System.getProperty("develMode", "false").toBoolean
}
