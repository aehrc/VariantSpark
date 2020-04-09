package au.csiro.pbdava.ssparkle.common.utils

trait Prof extends Logging {

  lazy val profEnable: Boolean = System.getProperty("sparkle.prof", "false").toBoolean

  var prevStartTime = 0L

  def profReset() = if (!profEnable) {} else {
    prevStartTime = System.currentTimeMillis()
  }

  def profPoint(msg: => String) = if (!profEnable) {} else {
    val duration = System.currentTimeMillis() - prevStartTime
    logInfo(s"${msg}: ${duration}")
    profReset()
  }

  def profIt[R](msg: => String)(f: => R): R =
    if (!profEnable) f
    else {
      val st = System.currentTimeMillis()
      val r = f
      val duration = System.currentTimeMillis() - st
      logInfo(s"${msg}: ${duration}")
      r
    }

}
