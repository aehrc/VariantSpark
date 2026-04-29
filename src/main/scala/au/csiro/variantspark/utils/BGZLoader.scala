package au.csiro.variantspark.utils

import au.csiro.pbdava.ssparkle.spark.SparkApp
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.seqdoop.hadoop_bam.util.{BGZFCodec, BGZFEnhancedGzipCodec}
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat

object BGZLoader {
  def textFile(sc: SparkContext, inputFile: String, nPartitions: Int = 0): RDD[String] = {
    val conf = sc.hadoopConfiguration
    val isBGZ = FileUtils.isBGZFile(inputFile, conf)
    println(inputFile + " is loading to spark RDD, isBGZFile: " + isBGZ)
    if (isBGZ) {
      val bgzfCodec = classOf[BGZFCodec].getCanonicalName
      val bgzfEnhancedCodec = classOf[BGZFEnhancedGzipCodec].getCanonicalName
      val existingCodecs =
        Option(conf.getStrings("io.compression.codecs")).getOrElse(Array.empty[String])
      val newCodecs = existingCodecs
        .filterNot(_ == bgzfCodec)
        .filterNot(_ == bgzfEnhancedCodec)
        .filterNot(_ == classOf[org.apache.hadoop.io.compress.GzipCodec].getCanonicalName)
      // BGZFEnhancedGzipCodec must be registered to override GzipCodec for .gz BGZF files
      conf.setStrings("io.compression.codecs", (newCodecs :+ bgzfEnhancedCodec :+ bgzfCodec): _*)
      // For BGZ files, control split count via max split size if nPartitions requested
      if (nPartitions > 0) {
        val fs = org.apache.hadoop.fs.FileSystem.get(conf)
        val fileLen = fs.getFileStatus(new org.apache.hadoop.fs.Path(inputFile)).getLen
        val maxSplitSize = Math.max(1L, fileLen / nPartitions)
        conf.setLong("mapreduce.input.fileinputformat.split.maxsize", maxSplitSize)
      }
      sc.newAPIHadoopFile[LongWritable, Text, TextInputFormat](inputFile,
          classOf[TextInputFormat], classOf[LongWritable], classOf[Text], conf)
        .map(_._2.toString)
    } else {
      // The standard GZIP libraries can handle files compressed as a whole
      // load .vcf, .vcf.gz or .vcf.bz2 to RDD
      if (nPartitions > 0) sc.textFile(inputFile, nPartitions)
      else sc.textFile(inputFile)
    }
  }
}
