package au.csiro.variantspark.input

import org.junit.Assert._
import org.junit.Test

class DefVariantToFeatureConverterTest {

  // Genotype encoding: 0=hom-ref, 1=het, 2=hom-alt, -1=missing
  val biallelicVariant: Variant = Variant(label = "chr1_10_T_A",
    genotypes = Array(Missing.BYTE_NA_VALUE, 0.toByte, 1.toByte, 2.toByte))

  val multiallelicVariant: Variant = Variant(label = "chr1_10_T_A|G",
    genotypes = Array(Missing.BYTE_NA_VALUE, 0.toByte, 1.toByte, 2.toByte))

  val expectedEncodedGenotype = Array(0.toByte, 0.toByte, 1.toByte, 2.toByte)

  @Test
  def testConvertsBialleicVariantCorrctly() {
    val converter = DefVariantToFeatureConverter(true, "_")
    val result = converter.convertZeroImputed(biallelicVariant)
    assertEquals("chr1_10_T_A", result.label)
    assertArrayEquals(expectedEncodedGenotype, result.valueAsByteArray)
  }

  @Test
  def testConvertsMultialleicVariantCorrctly() {
    val converter = DefVariantToFeatureConverter(false)
    val result = converter.convertZeroImputed(multiallelicVariant)
    assertEquals("chr1_10_T_A|G", result.label)
    assertArrayEquals(expectedEncodedGenotype, result.valueAsByteArray)
  }
}
