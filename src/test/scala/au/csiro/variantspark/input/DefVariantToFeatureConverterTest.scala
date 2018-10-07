package au.csiro.variantspark.input


import org.junit.Assert._
import org.junit.Test
import htsjdk.variant.variantcontext.VariantContext
import htsjdk.variant.variantcontext.VariantContextBuilder
import java.util.Collections
import htsjdk.variant.variantcontext.Genotype
import htsjdk.variant.variantcontext.GenotypeBuilder
import java.util.Arrays
import htsjdk.variant.variantcontext.Allele

class DefVariantToFeatureConverterTest {
  

  val refAllele = Allele.create('T'.toByte, true)
  val altAllele = Allele.create('A'.toByte, false)
  val altAllele1 = Allele.create('G'.toByte, false)
  
  
  val bialellicVC:VariantContext = new VariantContextBuilder("", "chr1", 10L, 10L, Arrays.asList(refAllele, altAllele))
      .genotypes(
          GenotypeBuilder.create("NC-NC", Arrays.asList(Allele.NO_CALL,Allele.NO_CALL)),
          GenotypeBuilder.create("REF-REF", Arrays.asList(refAllele,refAllele)),
          GenotypeBuilder.create("ALT-REF", Arrays.asList(altAllele,refAllele)),
          GenotypeBuilder.create("ALT-ALT", Arrays.asList(altAllele,altAllele))
       ).make()

  val multialleciVC:VariantContext = new VariantContextBuilder("", "chr1", 10L, 10L, Arrays.asList(refAllele, altAllele, altAllele1))
      .genotypes(
          GenotypeBuilder.create("NC-NC", Arrays.asList(Allele.NO_CALL,Allele.NO_CALL)),
          GenotypeBuilder.create("REF-REF", Arrays.asList(refAllele,refAllele)),
          GenotypeBuilder.create("ALT1-REF", Arrays.asList(altAllele1,refAllele)),
          GenotypeBuilder.create("ALT1-ALT", Arrays.asList(altAllele1,altAllele))
       ).make()
       
  val expectedEncodedGenotype = Array(0.toByte, 0.toByte, 1.toByte, 2.toByte)
  
  @Test
  def testConvertsBialleicVariantCorrctly() {
    val converter = DefVariantToFeatureConverter(true, ":")
    val result = converter.convert(bialellicVC)
    assertEquals("chr1:10:T:A", result.label)
    assertArrayEquals(expectedEncodedGenotype, result.values)
  }

  @Test
  def testConvertsMultialleicVariantCorrctly() {
    val converter = DefVariantToFeatureConverter(false)
    val result = converter.convert(multialleciVC)
    assertEquals("chr1_10", result.label)
    assertArrayEquals(expectedEncodedGenotype, result.values)
  }

  @Test(expected=classOf[IllegalArgumentException])
  def testFailesForMutlialleicVariantWithBiallelicConveter() {
    val converter = DefVariantToFeatureConverter(true)
    val result = converter.convert(multialleciVC)
  }

}