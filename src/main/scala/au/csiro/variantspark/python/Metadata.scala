package au.csiro.variantspark.python

import java.util

import au.csiro.pbdava.ssparkle.common.utils.VersionInfo

import scala.collection.JavaConverters._

object Metadata {
  def version(): String = VersionInfo.version
  def gitProperties(): java.util.Map[java.lang.Object, java.lang.Object] = {
    new util.HashMap[Object, Object](VersionInfo.gitProperties
        .asInstanceOf[Map[Object, Object]]
        .asJava)
  }

  def gitPropertiesAsString(): String =
    VersionInfo.gitProperties.map(_.toString()).mkString("\n")
}
