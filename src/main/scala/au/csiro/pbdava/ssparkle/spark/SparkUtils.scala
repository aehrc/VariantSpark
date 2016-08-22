package au.csiro.pbdava.ssparkle.spark

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast
import scala.reflect.ClassTag

object SparkUtils {
  
  
  
  def withBrodcast[T, R](sc:SparkContext)(v:T)(f: Broadcast[T] => R)(implicit ev:ClassTag[T]) = {
    val br = sc.broadcast(v)(ev)
    try {
      f(br)
    } finally {
      br.destroy()
    }
  }
  
  implicit def rdd2sc(rdd:RDD[_]) = rdd.sparkContext
}