package au.csiro.variantspark.hail

import au.csiro.variantspark.hail.variant.phased.PhasedGenericRecordReader
import is.hail.HailContext
import is.hail.io.vcf.LoadVCF
import is.hail.variant.GenericDataset
import is.hail.utils._
import org.apache.spark.rdd.RDD
import is.hail.variant.Variant


class VSHailContextFunctions(val hc:HailContext) extends AnyVal {
    
  def importVCFGenericEx(file: String, force: Boolean = false,
    forceBGZ: Boolean = false,
    headerFile: Option[String] = None,
    nPartitions: Option[Int] = None,
    dropSamples: Boolean = false,
    callFields: Set[String] = Set.empty[String]): GenericDataset = {
    importVCFsGenericEx(List(file), force, forceBGZ, headerFile, nPartitions, dropSamples, callFields)
  }

  
  def importVCFSnps(files: Seq[String], forceBGZ: Boolean = false, 
      nPartitions: Option[Int] = None):RDD[Variant] = {

    val inputs = LoadVCF.globAllVCFs(hc.hadoopConf.globAll(files), hc.hadoopConf,  forceBGZ)
    val codecs = hc.sc.hadoopConfiguration.get("io.compression.codecs")
    if (forceBGZ)
      hc.hadoopConf.set("io.compression.codecs",
        codecs.replaceAllLiterally("org.apache.hadoop.io.compress.GzipCodec", "is.hail.io.compress.BGzipCodecGZ"))
    
    val lines = hc.sc.textFilesLines(files.toArray, nPartitions.getOrElse(hc.sc.defaultMinPartitions))
    val partitionFile = lines.partitions.map(partitionPath)

    val variants = lines
      .filter(_.map { line =>
        !line.isEmpty &&
          line(0) != '#' &&
          LoadVCF.lineRef(line).forall(c => c == 'A' || c == 'C' || c == 'G' || c == 'T')
      }.value)
      .map(_.map(LoadVCF.lineVariant).value)
      
    hc.hadoopConf.set("io.compression.codecs", codecs)
    variants
  }
  
  def importVCFsGenericEx(files: Seq[String], force: Boolean = false,
    forceBGZ: Boolean = false,
    headerFile: Option[String] = None,
    nPartitions: Option[Int] = None,
    dropSamples: Boolean = false,
    callFields: Set[String] = Set.empty[String]): GenericDataset = {

    val inputs = LoadVCF.globAllVCFs(hc.hadoopConf.globAll(files), hc.hadoopConf, force || forceBGZ)

    val header = headerFile.getOrElse(inputs.head)

    val codecs = hc.sc.hadoopConfiguration.get("io.compression.codecs")

    if (forceBGZ)
      hc.hadoopConf.set("io.compression.codecs",
        codecs.replaceAllLiterally("org.apache.hadoop.io.compress.GzipCodec", "is.hail.io.compress.BGzipCodecGZ"))

    val reader = new PhasedGenericRecordReader(callFields)
    val gds = LoadVCF(hc, reader, header, inputs, nPartitions, dropSamples)

    hc.hadoopConf.set("io.compression.codecs", codecs)

    gds
  } 

  
}

