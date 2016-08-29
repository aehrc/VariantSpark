package au.csiro.variantspark.output

import au.csiro.variantspark.input.FeatureSource
import au.csiro.pbdava.ssparkle.common.utils.LoanUtils._
import com.github.tototoshi.csv.CSVWriter
import java.io.FileWriter

/**
 * This only works for smallish datasets (used local files)
 */
case class CSVFeatureSink(val fileName:String) {
  
  def save(fs:FeatureSource)  {
    withCloseable(new CSVWriter(new FileWriter(fileName))) { csvWriter =>
      csvWriter.writeRow("" :: fs.sampleNames)
      csvWriter.writeAll(fs.features().collect().toSeq.map(f => f.label :: f.values.toList))
    }
  }
  
}