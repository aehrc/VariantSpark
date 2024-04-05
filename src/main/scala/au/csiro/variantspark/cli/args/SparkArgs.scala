package au.csiro.variantspark.cli.args

import org.kohsuke.args4j.Option
import au.csiro.pbdava.ssparkle.spark.SparkApp
import au.csiro.variantspark.utils.FileUtils
import org.apache.spark.rdd.RDD
import htsjdk.samtools.util.BlockCompressedInputStream
import org.apache.hadoop.fs.Path
import java.io.File

trait SparkArgs extends SparkApp {

  @Option(name = "-sp", required = false, usage = "Spark parallelism (def=<default-spark-par>)",
    aliases = Array("--spark-par"))
  val sparkPar: Int = 0

  def textFile(inputFile: String): RDD[String] = {
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
      sc.textFile(inputFile, if (sparkPar > 0) sparkPar else sc.defaultParallelism)
    }
  }
}
