package au.csiro.variantspark.input

import htsjdk.samtools.util.AbstractIterator
import htsjdk.tribble.readers.LineIterator
import htsjdk.variant.vcf.{VCFCodec, VCFHeader, VCFHeaderVersion}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import au.csiro.variantspark.utils.{BGZLoader, ByteParsingUtils}

class DelegatingLineIterator(val it: Iterator[String])
    extends AbstractIterator[String] with LineIterator {

  override def advance(): String = if (it.hasNext) it.next() else null
}

class ExtendedVCFCodec extends VCFCodec {
  override def getVersion: VCFHeaderVersion = this.version
}

case class HeaderAndVersion(header: VCFHeader, version: VCFHeaderVersion)

/** Lightweight representation of a VCF variant with genotypes.
  * This is much more memory-efficient than htsjdk's VariantContext
  * as it only stores the essential fields needed for analysis.
  *
  * @param label the variant label (format: contig_position_ref_alt)
  * @param genotypes encoded genotype values: 0=hom-ref, 1=het, 2=hom-alt, -1=missing
  */
case class Variant(label: String, genotypes: Array[Byte])

/** VCF data source using byte-based parsing for memory efficiency on BGZ files,
  * and String-based parsing for non-BGZ files to avoid wasteful conversions.
  */
class VCFSource private (private val bytesData: Option[RDD[Array[Byte]]],
    private val stringData: Option[RDD[String]], val headerLines: Int = 500,
    val sparkPar: Int = 0) {

  lazy val headerAndVersion: HeaderAndVersion = {
    val codec = new ExtendedVCFCodec()
    // Take header lines and convert to strings for htsjdk parsing
    val headerStrings = stringData match {
      case Some(data) => data.take(headerLines).toIterator
      case None => bytesData.get.take(headerLines).map(b => new String(b, "UTF-8")).toIterator
    }
    val header: VCFHeader = codec
      .readActualHeader(new DelegatingLineIterator(headerStrings))
      .asInstanceOf[VCFHeader]
    HeaderAndVersion(header, codec.getVersion)
  }

  def header: VCFHeader = headerAndVersion.header
  def version: VCFHeaderVersion = headerAndVersion.version

  def genotypes(): RDD[Variant] = stringData match {
    case Some(data) => VCFSource.computeGenotypesFromStrings(data, sparkPar)
    case None => VCFSource.computeGenotypes(bytesData.get, sparkPar)
  }
}

object VCFSource {

  // VCF-specific byte constants
  private val G: Byte = 'G'.toByte
  private val T: Byte = 'T'.toByte

  def fromBytes(data: RDD[Array[Byte]], headerLines: Int, sparkPar: Int = 0): VCFSource =
    new VCFSource(Some(data), None, headerLines, sparkPar)

  def fromStrings(data: RDD[String], headerLines: Int, sparkPar: Int = 0): VCFSource =
    new VCFSource(None, Some(data), headerLines, sparkPar)

  def apply(sc: SparkContext, fileName: String, sparkPar: Int, headerLines: Int): VCFSource = {
    val conf = sc.hadoopConfiguration
    val isBGZ = au.csiro.variantspark.utils.FileUtils.isBGZFile(fileName, conf)

    val numPartitions = if (sparkPar > 0) sparkPar else sc.defaultParallelism

    if (isBGZ) {
      // For BGZ files, use byte-based parsing for memory efficiency
      val data = BGZLoader.bytesFile(sc, fileName)
      new VCFSource(Some(data), None, headerLines, numPartitions)
    } else {
      // For non-BGZ files, use String-based parsing to avoid wasteful conversions
      val data = BGZLoader.textFile(sc, fileName)
      new VCFSource(None, Some(data), headerLines, numPartitions)
    }
  }

  def apply(sc: SparkContext, fileName: String, sparkPar: Int): VCFSource =
    apply(sc, fileName, sparkPar, 500)

  def apply(sc: SparkContext, fileName: String): VCFSource =
    apply(sc, fileName, 0, 500)

  /** Computes variants using byte-based parsing with hash-based repartitioning.
    * Hash repartitioning ensures even distribution of variants across partitions and
    * deterministic partition assignment for efficient RDD unions.
    */
  private def computeGenotypes(data: RDD[Array[Byte]], numPartitions: Int): RDD[Variant] = {
    val variants = data
      .filter(b => b.length == 0 || b(0) != ByteParsingUtils.HASH)
      .mapPartitions { iter => iter.map(b => parseLineBytes(b)) }

    val targetPartitions =
      if (numPartitions > 0) numPartitions else data.sparkContext.defaultParallelism

    variants
      .keyBy(v => au.csiro.variantspark.utils.MurMur3Hash.hashString(v.label))
      .partitionBy(new org.apache.spark.HashPartitioner(targetPartitions))
      .values
  }

  /** Computes variants from String RDD (for non-BGZ files) with hash-based repartitioning.
    * Hash repartitioning ensures even distribution of variants across partitions and
    * deterministic partition assignment.
    */
  private def computeGenotypesFromStrings(data: RDD[String], numPartitions: Int): RDD[Variant] = {
    val variants = data
      .filter(line => line.isEmpty || !line.startsWith("#"))
      .mapPartitions { iter => iter.map(line => parseLine(line)) }

    if (numPartitions > 0) {
      variants
        .keyBy(v => au.csiro.variantspark.utils.MurMur3Hash.hashString(v.label))
        .partitionBy(new org.apache.spark.HashPartitioner(numPartitions))
        .values
    } else {
      variants
    }
  }

  /** Parses a VCF line from raw bytes into a Variant.
    * VCF format: CHROM POS ID REF ALT QUAL FILTER INFO FORMAT SAMPLE1 SAMPLE2 ...
    * Indices:      0    1   2   3   4    5     6      7     8      9      10  ...
    */
  def parseLineBytes(line: Array[Byte]): Variant = {
    import ByteParsingUtils._

    val len = line.length
    var pos = 0
    var tabIdx = findByte(line, TAB, pos, len)

    // Build label: contig_position_ref_alt
    val labelBuilder = new StringBuilder()

    // Field 0: CHROM
    labelBuilder.append(byteString(line, 0, tabIdx)).append('_')

    // Field 1: POS
    pos = tabIdx + 1
    tabIdx = findByte(line, TAB, pos, len)
    labelBuilder.append(byteString(line, pos, tabIdx)).append('_')

    // Field 2: ID (skip)
    pos = tabIdx + 1
    tabIdx = findByte(line, TAB, pos, len)

    // Field 3: REF
    pos = tabIdx + 1
    tabIdx = findByte(line, TAB, pos, len)
    labelBuilder.append(byteString(line, pos, tabIdx)).append('_')

    // Field 4: ALT
    pos = tabIdx + 1
    tabIdx = findByte(line, TAB, pos, len)
    labelBuilder.append(byteString(line, pos, tabIdx))

    val label = labelBuilder.toString()

    // Fields 5-7: QUAL, FILTER, INFO (skip)
    pos = tabIdx + 1
    tabIdx = findByte(line, TAB, pos, len) // QUAL
    pos = tabIdx + 1
    tabIdx = findByte(line, TAB, pos, len) // FILTER
    pos = tabIdx + 1
    tabIdx = findByte(line, TAB, pos, len) // INFO

    // Field 8: FORMAT
    pos = tabIdx + 1
    tabIdx = findByte(line, TAB, pos, len)
    val formatEnd = if (tabIdx < 0) len else tabIdx
    val gtIndex = findGTIndexBytes(line, pos, formatEnd)

    // Count samples and parse genotypes
    if (tabIdx < 0 || tabIdx >= len - 1) {
      // No samples
      return Variant(label, Array.empty[Byte])
    }

    // Count samples by counting remaining tabs
    // TODO: Pass nSamples as parameter instead of computing per-line
    // Sample count is constant for all lines (from VCF header), so broadcasting
    // from driver would avoid redundant computation in each executor task
    val nSamples = countSamplesBytes(line, tabIdx, len)
    val genotypes = new Array[Byte](nSamples)

    if (gtIndex >= 0) {
      pos = tabIdx + 1
      var i = 0
      while (i < nSamples) {
        tabIdx = findByte(line, TAB, pos, len)
        val sampleEnd = if (tabIdx < 0) len else tabIdx
        genotypes(i) = parseGenotypeBytes(line, pos, sampleEnd, gtIndex)
        pos = tabIdx + 1
        i += 1
      }
    } else {
      java.util.Arrays.fill(genotypes, Missing.BYTE_NA_VALUE)
    }

    Variant(label, genotypes)
  }

  /** Find the index of GT in the FORMAT field from bytes */
  @inline private def findGTIndexBytes(line: Array[Byte], start: Int, end: Int): Int = {
    import ByteParsingUtils.COLON
    // Check if FORMAT starts with "GT"
    if (end - start >= 2 && line(start) == G && line(start + 1) == T &&
        (end - start == 2 || line(start + 2) == COLON)) {
      return 0
    }
    // Otherwise scan for :GT:
    var colonCount = 0
    var i = start
    while (i < end - 1) {
      if (line(i) == COLON) {
        colonCount += 1
        if (i + 2 < end && line(i + 1) == G && line(i + 2) == T &&
            (i + 3 >= end || line(i + 3) == COLON)) {
          return colonCount
        }
      }
      i += 1
    }
    -1
  }

  /** Count the number of samples from bytes */
  @inline private def countSamplesBytes(line: Array[Byte], formatTabIdx: Int, len: Int): Int = {
    ByteParsingUtils.countByte(line, ByteParsingUtils.TAB, formatTabIdx + 1, len) + 1
  }

  /** Parse genotype directly from bytes */
  @inline private def parseGenotypeBytes(line: Array[Byte], start: Int, end: Int,
      gtIndex: Int): Byte = {
    import ByteParsingUtils.{COLON, DOT, ZERO}

    var gtStart = start
    var gtEnd = end

    if (gtIndex == 0) {
      // GT is first field, find first colon or use end
      var i = start
      while (i < end && line(i) != COLON) i += 1
      gtEnd = i
    } else {
      // Skip to the right field
      var colonCount = 0
      var i = start
      while (i < end && colonCount < gtIndex) {
        if (line(i) == COLON) colonCount += 1
        i += 1
      }
      gtStart = i
      while (i < end && line(i) != COLON) i += 1
      gtEnd = i
    }

    // Parse the GT field directly
    // TODO: Improve multi-allelic variant handling
    // Current limitation: genotypes like 1/2 are incorrectly coded as hom-alt (2)
    // when they should be het (1). Consider tracking specific allele values to
    // properly distinguish between 1/1 (hom-alt) and 1/2 (het with different alts).
    var hasRef = false
    var hasAlt = false
    var hasMissing = false
    var i = gtStart
    while (i < gtEnd) {
      val c = line(i)
      if (c == DOT) hasMissing = true
      else if (c == ZERO) hasRef = true
      else if (c >= '1' && c <= '9') hasAlt = true
      i += 1
    }

    if (hasMissing) Missing.BYTE_NA_VALUE
    else if (hasRef && hasAlt) 1.toByte
    else if (hasAlt) 2.toByte
    else if (hasRef) 0.toByte
    else Missing.BYTE_NA_VALUE
  }

  /** Parse a VCF line from String (legacy compatibility) */
  def parseLine(line: String): Variant = {
    parseLineBytes(line.getBytes("UTF-8"))
  }

  // TODO: Reinstate String-based parsing for uncompressed VCFs to reduce overhead
  /** Parses a genotype field from a VCF sample column.
    * Handles both phased (|) and unphased (/) genotypes.
    */
  def parseGenotype(sample: String, gtIndex: Int): Byte = {
    val bytes = sample.getBytes("UTF-8")
    parseGenotypeBytes(bytes, 0, bytes.length, gtIndex)
  }
}
