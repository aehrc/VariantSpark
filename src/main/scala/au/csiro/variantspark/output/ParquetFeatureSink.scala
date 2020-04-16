package au.csiro.variantspark.output

import au.csiro.pbdava.ssparkle.common.utils.SerialUtils
import au.csiro.variantspark.input.FeatureSource
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SaveMode
import org.apache.spark.rdd.RDD
import au.csiro.variantspark.data._

class ParquetFeatureSink(val outputPath: String) extends FeatureSink {

  def save(featureSource: FeatureSource) {
    val features = featureSource.features
    val sqlContext = new org.apache.spark.sql.SQLContext(features.sparkContext)

    import sqlContext.implicits._
    // TODO: Hack (need to fix that to work on any feature type""
    // As Well as I need to create encoding for feature type
    // I guess I will encode it on long with the first int being the type and the next
    // extra information about the type
    features.asInstanceOf[RDD[StdFeature]].toDF.write.mode(SaveMode.Overwrite).save(outputPath)

    val fs = FileSystem.get(features.sparkContext.hadoopConfiguration)

    val columns: List[String] = featureSource.sampleNames
    SerialUtils.write(columns, fs.create(new Path(outputPath, "_columns")))
  }
}
