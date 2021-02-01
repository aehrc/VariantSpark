package au.csiro.pbdava.ssparkle.common.utils

import java.util.Properties

import scala.collection.JavaConverters._

object VersionInfo {
  lazy val gitProperties: Map[String, String] =
    LoanUtils.withCloseable(getClass.getClassLoader.getResourceAsStream("git.properties")) { in =>
      if (in != null) {
        val gitProperties = new Properties()
        gitProperties.load(in)
        gitProperties.asScala.toMap
      } else {
        throw new IllegalStateException("Cannot find 'git.properties'")
      }
    }

  lazy val version: String = gitProperties.getOrElse("git.build.version",
    throw new IllegalStateException("Cannot retrieve version information"))
}
