package au.csiro.pbdava.ssparkle.common.utils

class Timed {
  var elapsedTime:Long = 0
  
  def time[R](c: => R):R = {
    val startTime = System.currentTimeMillis()
    val result = c 
    elapsedTime = System.currentTimeMillis() - startTime
    result
  }
}

object Timed {
  def time[R](c: => R):(R,Long) = {
    val timed = new Timed()
    val r = timed.time(c)
    (r, timed.elapsedTime)
  }
}