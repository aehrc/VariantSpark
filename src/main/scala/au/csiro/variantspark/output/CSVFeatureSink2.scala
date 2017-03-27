package au.csiro.variantspark.output

import au.csiro.variantspark.input.FeatureSource
import au.csiro.pbdava.ssparkle.common.utils.LoanUtils._
import com.github.tototoshi.csv.CSVWriter
import java.io.FileWriter

/**
 * This only works for smallish datasets (used local files)
 */
case class CSVFeatureSink2(val fileName:String) extends FeatureSink {
  
  def save(fs:FeatureSource)  {
    
    val rows = fs.features().map( f => (f.label :: f.values.map(_.toString).toList).mkString(","))
    val header = fs.features.sparkContext.parallelize(List(("" :: fs.sampleNames).mkString(",")))
    header.union(rows).coalesce(1, true).saveAsTextFile(fileName)
  }  
}