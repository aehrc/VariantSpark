package au.csiro.variantspark.utils

import au.csiro.pbdava.ssparkle.spark.SparkApp
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.seqdoop.hadoop_bam.util.BGZFCodec
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat

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
}
