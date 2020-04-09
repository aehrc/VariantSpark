package au.csiro.pbdava.ssparkle.spark

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast
import scala.reflect.ClassTag
import org.apache.spark.storage.StorageLevel

object SparkUtils {

  def withCached[T, R](rdd: RDD[T])(f: RDD[T] => R) = {
    val manageCaching = rdd.getStorageLevel == StorageLevel.NONE
    try {
      if (manageCaching) {
        rdd.persist()
      }
      f(rdd)
    } finally {
      if (manageCaching) {
        rdd.unpersist()
      }
    }
  }

  def withBroadcast[T, R](sc: SparkContext)(v: T)(f: Broadcast[T] => R)(
      implicit ev: ClassTag[T]) = {
    val br = sc.broadcast(v)(ev)
    try {
      f(br)
    } finally {
      br.destroy()
    }
  }

  // TODO (review RDD for update to DF/DS)
  implicit def rdd2sc(rdd: RDD[_]) = rdd.sparkContext
}
