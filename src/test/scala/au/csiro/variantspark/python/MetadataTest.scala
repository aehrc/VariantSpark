package au.csiro.variantspark.python

import java.util

import org.junit.Test

class MetadataTest {

  @Test
  def testVersionInfo(): Unit = {
    val gitProperties: util.Map[Object, Object] = Metadata.gitProperties()
  }
}
