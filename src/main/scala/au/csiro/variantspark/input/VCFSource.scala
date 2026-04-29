package au.csiro.variantspark.input

import htsjdk.samtools.util.AbstractIterator
import htsjdk.tribble.readers.LineIterator
import htsjdk.variant.vcf.{VCFCodec, VCFHeader, VCFHeaderVersion}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import au.csiro.variantspark.utils.BGZLoader

class DelegatingLineIterator(val it: Iterator[String])
    extends AbstractIterator[String] with LineIterator {

  override def advance(): String =
    if (it.hasNext) it.next() else null
}

class ExtendedVCFCodec extends VCFCodec {
  override def getVersion: VCFHeaderVersion = this.version
}

case class HeaderAndVersion(header: VCFHeader, version: VCFHeaderVersion)

/** Lightweight representation of a VCF variant with genotypes.
  *
  * @param label contig_position_ref_alt
  * @param genotypes encoded genotype values:
  *                  0 = hom-ref
  *                  1 = het (for this specific ALT)
  *                  2 = hom-alt (for this specific ALT)
  *                 -1 = missing
  */
case class Variant(label: String, genotypes: Array[Byte])

class VCFSource(val lines: RDD[String], val headerLines: Int = 500) {

  lazy val headerAndVersion: HeaderAndVersion = {
    val codec = new ExtendedVCFCodec()
    val header: VCFHeader =
      codec
        .readActualHeader(new DelegatingLineIterator(
              lines.filter(_.startsWith("#")).take(headerLines).toIterator))
        .asInstanceOf[VCFHeader]

    HeaderAndVersion(header, codec.getVersion)
  }

  def header: VCFHeader = headerAndVersion.header
  def version: VCFHeaderVersion = headerAndVersion.version

  def genotypes(): RDD[Variant] =
    VCFSource.computeGenotypes(lines, headerAndVersion)
}

object VCFSource {

  def apply(sc: SparkContext, fileName: String, nPartitions: Int, headerLines: Int): VCFSource = {

    val numPartitions =
      if (nPartitions > 0) nPartitions else sc.defaultParallelism

    val data = BGZLoader.textFile(sc, fileName, numPartitions)
    new VCFSource(data, headerLines)
  }

  def apply(sc: SparkContext, fileName: String, nPartitions: Int): VCFSource =
    apply(sc, fileName, nPartitions, 500)

  def apply(sc: SparkContext, fileName: String): VCFSource =
    apply(sc, fileName, 0, 500)

  private def computeGenotypes(lines: RDD[String],
      headerAndVersion: HeaderAndVersion): RDD[Variant] = {

    lines
      .mapPartitions { iter =>
        iter
          .filter(l => !l.startsWith("#"))
          .flatMap(parseLineMulti)
      }
  }

  /** Parses a VCF line and splits multiallelics into separate biallelic Variants */
  def parseLineMulti(line: String): Seq[Variant] = {
    val fields = line.split("\t", -1)

    val chrom = fields(0)
    val pos = fields(1)
    val ref = fields(3)
    val alts = fields(4).split(",")

    if (fields.length <= 9) {
      return Seq.empty
    }

    val formatField = fields(8)
    val gtIndex = formatField.split(":").indexOf("GT")

    val nSamples = fields.length - 9
    val rawGenotypes = new Array[String](nSamples)

    // Extract genotype strings once
    var i = 0
    while (i < nSamples) {
      val sample = fields(9 + i)

      val gtField =
        if (gtIndex == 0) {
          val colonIdx = sample.indexOf(':')
          if (colonIdx < 0) sample else sample.substring(0, colonIdx)
        } else if (gtIndex > 0) {
          sample.split(":")(gtIndex)
        } else {
          "."
        }

      rawGenotypes(i) = gtField
      i += 1
    }

    // Create one Variant per ALT allele (SNPs only)
    alts.zipWithIndex.flatMap {
      case (alt, altIdx) =>
        // Filter out indels/SVs - only keep biallelic SNPs
        if (ref.length == 1 && alt.length == 1 && !alt.startsWith("<")) {
          val label = s"${chrom}_${pos}_${ref}_${alt}"
          val genotypes = new Array[Byte](nSamples)

          var j = 0
          while (j < nSamples) {
            genotypes(j) = encodeBiallelic(rawGenotypes(j), altIdx + 1)
            j += 1
          }

          Some(Variant(label, genotypes))
        } else {
          None // Skip indels and structural variants
        }
    }
  }

  /** Encodes genotype relative to a specific ALT allele.
    *
    * targetAlt:
    *   1 = first ALT
    *   2 = second ALT
    *   ...
    *
    * Returns:
    *   0, 1, 2 or Missing.BYTE_NA_VALUE
    */
  @inline private def encodeBiallelic(gt: String, targetAlt: Int): Byte = {

    var alleleCount = 0
    var hasMissing = false

    var current = 0
    var readingNumber = false
    var i = 0

    // Iterate including sentinel to flush last allele
    while (i <= gt.length) {

      val c =
        if (i < gt.length) gt.charAt(i)
        else '/'

      if (c == '.') {
        hasMissing = true
      } else if (c >= '0' && c <= '9') {
        current = current * 10 + (c - '0')
        readingNumber = true
      } else {
        if (readingNumber) {
          if (current == targetAlt) alleleCount += 1
          current = 0
          readingNumber = false
        }
      }

      i += 1
    }

    if (hasMissing) Missing.BYTE_NA_VALUE
    else alleleCount.toByte
  }
}
