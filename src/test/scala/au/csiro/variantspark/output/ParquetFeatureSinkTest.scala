package au.csiro.variantspark.output

import au.csiro.variantspark.input.ParquetFeatureSource
import au.csiro.variantspark.input.generate.OrdinalFeatureGenerator
import au.csiro.variantspark.test.SparkTest
import org.junit.Assert._
import org.junit.Test
import org.junit.Ignore

class ParquetFeatureSinkTest extends SparkTest {

  //TODO:[Variables] Fix
  @Ignore
  @Test
  def testSavesData() {
    val originalSource = new OrdinalFeatureGenerator(3, 100, 10)
    val sink = new ParquetFeatureSink("target/parquet-sink")
    sink.save(originalSource)
    val source = new ParquetFeatureSource("target/parquet-sink")
    assertEquals(originalSource.sampleNames, source.sampleNames)
    assertArrayEquals(
        originalSource.features.collect().asInstanceOf[Array[Object]],
        source.features.collect().asInstanceOf[Array[Object]])
  }
}
