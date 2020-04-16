package au.csiro.variantspark.input

import au.csiro.pbdava.ssparkle.common.utils.SerialUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import au.csiro.variantspark.data._
import au.csiro.variantspark.data.StdFeature

@deprecated
case class ParquetFeatureSource(inputPath: String)(implicit sc: SparkContext)
    extends FeatureSource {

  override lazy val sampleNames: List[String] = {
    val pathToColumns = new Path(inputPath, "_columns")
    val fs = FileSystem.get(pathToColumns.toUri, sc.hadoopConfiguration)
    SerialUtils.read(fs.open(pathToColumns))
  }

  def features: RDD[Feature] = {
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val rawDF = sqlContext.read.parquet(inputPath)
    rawDF.rdd.map(r => StdFeature.from(r.getString(0), r.getAs[Array[Byte]](1)))
  }
}
