package au.csiro.variantspark.utils

import au.csiro.pbdava.ssparkle.spark.SparkApp
import org.apache.spark.rdd.RDD
import htsjdk.samtools.util.BlockCompressedInputStream
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext

object BGZLoader {
  def textFile(sc: SparkContext, inputFile: String): RDD[String] = {
    val isBGZ = FileUtils.isBGZFile(inputFile)
    println(inputFile + " is loading to spark RDD, isBGZFile: " + isBGZ)
    if (isBGZ) {
      // BGZIP file is compressed as blocks, requires specialized libraries htsjdk
      val path = new Path(inputFile)
      val fs = path.getFileSystem(sc.hadoopConfiguration)
      val bgzInputStream = new BlockCompressedInputStream(fs.open(path))
      // each blocks can be decompressed independently and to be read in parallel
      sc.parallelize(Stream.continually(bgzInputStream.readLine()).takeWhile(_ != null).toList)
    } else {
      // The standard GZIP libraries can handle files compressed as a whole
      // load .vcf, .vcf.gz or .vcf.bz2 to RDD
      sc.textFile(inputFile)
    }
  }
}
