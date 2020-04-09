package au.csiro.variantspark.input

import htsjdk.samtools.util.AbstractIterator
import htsjdk.tribble.readers.LineIterator
import htsjdk.variant.vcf.{VCFCodec, VCFHeader, VCFHeaderVersion}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class DelegatingLineIterator(val it: Iterator[String])
    extends AbstractIterator[String]
    with LineIterator {

  override def advance(): String = if (it.hasNext) it.next() else null
}

class ExtendedVCFCodec extends VCFCodec {
  def getVersion() = this.version
}

case class HeaderAndVersion(header: VCFHeader, version: VCFHeaderVersion)

class VCFSource(val lines: RDD[String], val headerLines: Int = 500) {

  lazy val headerAndVersion = {
    val codec = new ExtendedVCFCodec()
    val header: VCFHeader = codec
      .readActualHeader(new DelegatingLineIterator(lines.take(headerLines).toIterator))
      .asInstanceOf[VCFHeader]
    HeaderAndVersion(header, codec.getVersion())
  }

  def header = headerAndVersion.header
  def version = headerAndVersion.version

  def genotypes() = VCFSource.computeGenotypes(lines, headerAndVersion)

}

object VCFSource {

  def apply(lines: RDD[String], headerLines: Int = 500): VCFSource =
    new VCFSource(lines, headerLines)
  def apply(sc: SparkContext, fileName: String, headerLines: Int): VCFSource =
    apply(sc.textFile(fileName), headerLines)
  def apply(sc: SparkContext, fileName: String): VCFSource = apply(sc.textFile(fileName))

  private def computeGenotypes(lines: RDD[String], headerAndVersion: HeaderAndVersion) = {
    val br_headerAndVersion = lines.context.broadcast(headerAndVersion)
    lines
      .filter(l => !l.startsWith("#"))
      .map(l => {
        val codec = new VCFCodec()
        val headerAndVersion = br_headerAndVersion.value
        codec.setVCFHeader(headerAndVersion.header, headerAndVersion.version)
        codec.decode(l)
      })
  }
}
