package au.csiro.variantspark.cli.args

import org.kohsuke.args4j.Option
import au.csiro.pbdava.ssparkle.spark.SparkApp
import org.apache.spark.rdd.RDD
import htsjdk.samtools.util.BlockCompressedInputStream
import org.apache.hadoop.fs.Path
import java.io.File

trait SparkArgs extends SparkApp {

  @Option(name = "-sp", required = false, usage = "Spark parallelism (def=<default-spark-par>)",
    aliases = Array("--spark-par"))
  val sparkPar: Int = 0

  def textFile(inputFile: String): RDD[String] = {
    val input = new File(inputFile)
    val isBGZ = input.getName.split('.').lastOption.getOrElse("").equalsIgnoreCase("bgz")
    println(inputFile + " is loading to spark RDD " + isBGZ)
    if (isBGZ) {
      val path = new Path(inputFile)
      val fs = path.getFileSystem(sc.hadoopConfiguration)
      val bgzInputStream = new BlockCompressedInputStream(fs.open(path))
      sc.parallelize(Stream.continually(bgzInputStream.readLine()).takeWhile(_ != null).toList)
    } else {
      sc.textFile(inputFile, if (sparkPar > 0) sparkPar else sc.defaultParallelism)
    }
  }
}
