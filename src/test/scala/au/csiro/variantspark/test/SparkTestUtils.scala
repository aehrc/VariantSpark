package au.csiro.variantspark.test

import org.apache.spark.sql.SparkSession

object SparkTestUtils {
  val MasterLocal2 = SparkSession.builder().master("local[2]")

  def withSessionBuilder(sessionBulider: SparkSession.Builder)(f: SparkSession => Unit) {
    var session: SparkSession = null
    try {
      session = sessionBulider.getOrCreate()
      f(session)
    } finally {
      if (session != null) {
        session.close()
      }
    }
  }
}
