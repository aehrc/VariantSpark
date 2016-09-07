package au.csiro.pbdava.ssparkle.common.utils



case class Timer() {
  val startTime = System.currentTimeMillis()
  lazy val duration = System.currentTimeMillis() - startTime
  def durationInSec = duration/1000.0
}

case class TimedResult[T](val result:T, val elapsedTime:Long) {
  def withResult(f:T=>Unit):TimedResult[T] = {
    f(result)
    this
  }
  def withResultAndTime(f:(T,Long)=>Unit):TimedResult[T] = {
    f(result, elapsedTime)
    this
  }
  
  def report(msg: =>String) = {
    println(s"${msg} time: ${elapsedTime}")
    this
  }
}

object Timed {
  def time[T](c: => T):TimedResult[T] =  {
    val startTime = System.currentTimeMillis()
    val result = c 
    val elapsedTime = System.currentTimeMillis() - startTime
    TimedResult(result, elapsedTime)
  }
}