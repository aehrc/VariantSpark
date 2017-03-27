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
    val header = ("" :: fs.sampleNames).mkString(",")
    fs.features().map( f => (f.label :: f.values.map(_.toString).toList).mkString(",")).mapPartitionsWithSplit({ case (i, it) =>
      if (i > 0) it else Some(header).iterator ++ it
    }).coalesce(1, true).saveAsTextFile(fileName)
  }  
}