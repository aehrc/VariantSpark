package au.csiro.variantspark.output

import au.csiro.variantspark.input.FeatureSource
import org.apache.spark.sql.SaveMode
import org.apache.hadoop.fs.FileSystem
import org.apache.commons.io.IOUtils
import au.csiro.pbdava.ssparkle.common.utils.LoanUtils
import java.io.ObjectOutputStream
import org.apache.hadoop.fs.Path
import au.csiro.pbdava.ssparkle.common.utils.SerialUtils

class ParquetFeatureSink(val outputPath:String) extends FeatureSink {
  
  def save(featureSource:FeatureSource)  {
     val features = featureSource.features
     val sqlContext = new org.apache.spark.sql.SQLContext(features.sparkContext)

     import sqlContext.implicits._    
     features.toDF.write.mode(SaveMode.Overwrite).save(outputPath)

     val fs = FileSystem.get(features.sparkContext.hadoopConfiguration)

     val columns:List[String] = featureSource.sampleNames
     SerialUtils.write(columns, fs.create(new Path(outputPath, "_columns")))
   }
}