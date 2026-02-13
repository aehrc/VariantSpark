package au.csiro.variantspark.utils

import au.csiro.pbdava.ssparkle.spark.SparkApp
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.seqdoop.hadoop_bam.util.BGZFCodec
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat

// TODO: Consider deprecating non-BGZ file support in future versions
// Non-block-compressed files (.vcf.gz, .vcf.bz2) have poor performance characteristics
// in distributed environments:
// - Cannot be split across partitions (processed by single executor)
// - Prevents efficient parallelisation for large datasets
//
// If removed, VCFSource should use String-based parsing (parseLine) for uncompressed VCFs
// Recommended: Encourage users to convert files to BGZ format for production use.

object BGZLoader {
  def textFile(sc: SparkContext, inputFile: String): RDD[String] = {
    val conf = sc.hadoopConfiguration
    val isBGZ = FileUtils.isBGZFile(inputFile, conf)
    println(inputFile + " is loading to spark RDD, isBGZFile: " + isBGZ)
    if (isBGZ) {
      val bgzfCodec = classOf[BGZFCodec].getCanonicalName
      val existingCodecs =
        Option(conf.getStrings("io.compression.codecs")).getOrElse(Array.empty[String])
      if (!existingCodecs.contains(bgzfCodec)) {
        conf.setStrings("io.compression.codecs", (bgzfCodec +: existingCodecs): _*)
      }
      sc.newAPIHadoopFile[LongWritable, Text, TextInputFormat](inputFile,
          classOf[TextInputFormat], classOf[LongWritable], classOf[Text], conf)
        .map(_._2.toString)
    } else {
      // The standard GZIP libraries can handle files compressed as a whole
      // load .vcf, .vcf.gz or .vcf.bz2 to RDD
      sc.textFile(inputFile)
    }
  }

  /** Load file as RDD of raw byte arrays.
    * This is more memory-efficient than String (UTF-8 vs UTF-16).
    * VCF files are ASCII, so each character is 1 byte instead of 2.
    */
  def bytesFile(sc: SparkContext, inputFile: String): RDD[Array[Byte]] = {
    val conf = sc.hadoopConfiguration
    val isBGZ = FileUtils.isBGZFile(inputFile, conf)
    println(inputFile + " is loading to spark RDD as bytes, isBGZFile: " + isBGZ)
    if (isBGZ) {
      val bgzfCodec = classOf[BGZFCodec].getCanonicalName
      val existingCodecs =
        Option(conf.getStrings("io.compression.codecs")).getOrElse(Array.empty[String])
      if (!existingCodecs.contains(bgzfCodec)) {
        conf.setStrings("io.compression.codecs", (bgzfCodec +: existingCodecs): _*)
      }
      sc.newAPIHadoopFile[LongWritable, Text, TextInputFormat](inputFile,
          classOf[TextInputFormat], classOf[LongWritable], classOf[Text], conf)
        .map {
          case (_, text) =>
            // Copy bytes from Text to avoid reuse issues
            java.util.Arrays.copyOf(text.getBytes, text.getLength)
        }
    } else {
      // For non-BGZ files, convert String to bytes
      sc.textFile(inputFile).map(_.getBytes("UTF-8"))
    }
  }
}
